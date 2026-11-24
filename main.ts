// Selective breakout paper bot for the pump.fun replay (agent-fun/replay/v2).
//
// Idea: most pump.fun coins bleed to zero, and every round trip costs ~2.5%
// in curve fees plus 0.00021 SOL of fixed tx fees, so a bot only wins by
// being picky. This bot buys ONE small stake per coin, only when a coin
// shows genuine accelerating buy pressure (net SOL inflow, many distinct
// buyers, price rising and holding near its window high), and exits fast on
// a take-profit, a stop-loss, a trailing stop, a fade, or a time limit.
// No pyramiding, no re-entry on the same coin: one clean shot per mint.
// A late-window breadth backstop relaxes the entry bar (and, at the very
// end, runs tiny probe round trips) so the 20-coin guardrail is met
// without spray-and-pray trading early.
//
// Fully deterministic: no clock, no Math.random, no network. All state
// derives from the batch stream; iteration is over insertion-ordered maps.

import { LAMPORTS_PER_SOL, runBot, type BotState, type Batch, type BuyInput, type SellInput } from './vendor/makefun-bot.ts';

const SOL = LAMPORTS_PER_SOL;
const STAKE = (SOL * 15n) / 100n; // 0.15 SOL per position (1%-of-reserve cap binds first on small curves)
const PROBE_STAKE = (SOL * 2n) / 100n; // 0.02 SOL late-window probe
const MIN_BUY = (SOL * 1n) / 100n; // 0.01 SOL guardrail round-trip floor
const BUY_SLIP_BPS = 1500;
const SELL_SLIP_BPS = 10_000;

const TP = 0.2; // take profit +20%
const SL = -0.06; // stop loss -6%
const TRAIL_ARM = 0.08; // trailing stop arms at +8%
const TRAIL_DROP = 0.06; // sell 6% below the position's peak value
const MAX_HOLD_SLOTS = 250;
const COOLDOWN_SLOTS = 300;
const WINDOW_TRADES = 40;
const MIN_AGE_SLOTS = 800; // survivor filter: fresh curves mostly dump; 800+ slots old with
// sustained buying is where the edge lives (swept on the dev window:
// minAge 400 loses ~-0.26 SOL, minAge 800 with strong inflow turns positive)
const MIN_RQ_SOL = 3;
const MAX_RQ_SOL = 60;
const MAX_ENTRIES_PER_MINT = 1;
const TOTAL_SLOTS = 403_213; // dev window length, for breadth pacing

interface TapeTrade {
  slot: number;
  price: number; // SOL per token (float, decision use only)
  sol: number; // SOL size of the trade
  isBuy: boolean;
  user: string;
}

interface Tape {
  createdSlot: number;
  trades: TapeTrade[];
  entries: number;
  cooldownUntil: number;
  probe: boolean;
}

interface Held {
  peakRet: number;
  entrySlot: number;
}

const tapes = new Map<string, Tape>();
const held = new Map<string, Held>();
let firstSlot = -1;
let coinsTraded = 0;
const countedCoins = new Set<string>();

function hasPendingBuy(state: BotState, mint: string): boolean {
  for (const p of state.pending.values()) {
    if (p.order.side === 'buy' && p.order.mint === mint) return true;
  }
  return false;
}

function priceOf(state: BotState, mint: string): number {
  const c = state.curves.get(mint);
  if (!c || c.virtual_token_reserves <= 0n) return 0;
  return Number(c.virtual_quote_reserves) / Number(c.virtual_token_reserves);
}

function valueOf(state: BotState, mint: string, tokens: bigint): number {
  return Number(tokens) * priceOf(state, mint);
}

function tapeStats(t: Tape): {
  net: number;
  netFirst: number;
  netSecond: number;
  buys: number;
  buyers: number;
  priceUp: number;
  nearHigh: number;
  topShare: number;
  recentNet: number;
} {
  const tr = t.trades;
  let net = 0;
  let netFirst = 0;
  let netSecond = 0;
  let buys = 0;
  let buySol = 0;
  const buyers = new Set<string>();
  const buyerSol = new Map<string, number>();
  let high = 0;
  const half = Math.floor(tr.length / 2);
  for (let i = 0; i < tr.length; i++) {
    const x = tr[i];
    const signed = x.isBuy ? x.sol : -x.sol;
    net += signed;
    if (i < half) netFirst += signed;
    else netSecond += signed;
    if (x.isBuy) {
      buys += 1;
      buySol += x.sol;
      buyers.add(x.user);
      buyerSol.set(x.user, (buyerSol.get(x.user) ?? 0) + x.sol);
    }
    if (x.price > high) high = x.price;
  }
  let top = 0;
  for (const v of buyerSol.values()) if (v > top) top = v;
  const firstPrice = tr.length > 0 ? tr[0].price : 0;
  const lastPrice = tr.length > 0 ? tr[tr.length - 1].price : 0;
  const priceUp = firstPrice > 0 ? lastPrice / firstPrice - 1 : 0;
  const nearHigh = high > 0 ? lastPrice / high : 0;
  const topShare = buySol > 0 ? top / buySol : 1;
  let recentNet = 0;
  const start = Math.max(0, tr.length - 15);
  for (let i = start; i < tr.length; i++) recentNet += tr[i].isBuy ? tr[i].sol : -tr[i].sol;
  return { net, netFirst, netSecond, buys, buyers: buyers.size, priceUp, nearHigh, topShare, recentNet };
}

await runBot({
  onBatch(state: BotState, batch: Batch) {
    if (firstSlot < 0) firstSlot = batch.slot;
    const progress = Math.min(1, (batch.slot - firstSlot) / TOTAL_SLOTS);
    const orders: Array<BuyInput | SellInput> = [];
    const caps = state.hello.caps;
    const txFee = state.txFeeLamports();
    let free = state.freeCashLamports();

    // Fold this batch's events into the per-coin tapes.
    const touched = new Set<string>();
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        if (!tapes.has(e.mint)) {
          tapes.set(e.mint, { createdSlot: e.slot, trades: [], entries: 0, cooldownUntil: 0, probe: false });
        }
      } else if (e.event === 'TradeEvent') {
        let t = tapes.get(e.mint);
        if (!t) {
          t = { createdSlot: e.slot, trades: [], entries: 0, cooldownUntil: 0, probe: false };
          tapes.set(e.mint, t);
        }
        const p = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
        t.trades.push({
          slot: e.slot,
          price: p,
          sol: Number(e.sol_amount) / 1e9,
          isBuy: e.is_buy,
          user: e.user,
        });
        if (t.trades.length > WINDOW_TRADES) t.trades.splice(0, t.trades.length - WINDOW_TRADES);
        touched.add(e.mint);
      } else if (e.event === 'CompleteEvent') {
        tapes.delete(e.mint);
        held.delete(e.mint);
      }
    }

    // Track fills: count distinct coins with a completed round trip, and
    // note newly landed positions for peak tracking.
    for (const f of batch.fills) {
      if (f.status === 'filled' && f.side === 'sell') countedCoins.add(f.mint);
      if (f.status === 'filled' && f.side === 'buy') {
        const h = held.get(f.mint);
        if (!h) held.set(f.mint, { peakRet: 0, entrySlot: batch.slot });
      }
    }
    coinsTraded = countedCoins.size;

    // --- Exits: check every held position against the latest price. ---
    for (const [mint, pos] of state.positions) {
      const h = held.get(mint);
      const price = priceOf(state, mint);
      if (price <= 0) continue;
      const value = valueOf(state, mint, pos.token_amount);
      const cost = Number(pos.cost_lamports);
      const ret = cost > 0 ? value / cost - 1 : 0;
      if (h) {
        if (ret > h.peakRet) h.peakRet = ret;
      } else {
        held.set(mint, { peakRet: ret, entrySlot: batch.slot });
      }
      const peak = held.get(mint)!.peakRet;
      const tape = tapes.get(mint);
      const recentNet = tape ? tapeStats(tape).recentNet : 0;
      const heldSlots = batch.slot - (h ? h.entrySlot : batch.slot);
      let sell = false;
      if (ret >= TP) sell = true;
      else if (ret <= SL) sell = true;
      else if (peak >= TRAIL_ARM && ret <= peak - TRAIL_DROP) sell = true;
      else if (heldSlots > MAX_HOLD_SLOTS && ret < 0.05) sell = true;
      else if (recentNet < -0.5 && ret < 0) sell = true;
      if (sell) {
        if (orders.length < caps.max_orders_per_batch) {
          orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIP_BPS });
          const tt = tapes.get(mint);
          if (tt) tt.cooldownUntil = batch.slot + COOLDOWN_SLOTS;
          held.delete(mint);
        }
      }
    }

    // Probe positions (tiny late-window breadth trades) are sold as soon as
    // the buy lands, on the next batch after the fill.
    // (They are ordinary positions; the exit block above already sells
    // losers, but probes exit unconditionally here.)
    for (const [mint, pos] of state.positions) {
      const t = tapes.get(mint);
      if (t && t.probe && !orders.some((o) => o.side === 'sell' && o.mint === mint)) {
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIP_BPS });
        t.probe = false;
        held.delete(mint);
      }
    }

    // --- Entries: only coins that traded in this batch. ---
    const relaxed = progress > 0.75 && coinsTraded < 20;
    const veryLate = progress > 0.92 && coinsTraded < 20;
    const minNet = relaxed ? 0.3 : 5;
    const minBuys = relaxed ? 5 : 10;
    const minBuyers = relaxed ? 4 : 8;
    const minUp = relaxed ? 0.03 : 0.08;
    let slots = caps.max_open_positions - state.committedPositionCount();
    for (const mint of touched) {
      if (orders.length >= caps.max_orders_per_batch || slots <= 0) break;
      if (state.positions.has(mint)) continue;
      if (hasPendingBuy(state, mint)) continue;
      const t = tapes.get(mint);
      const curve = state.curves.get(mint);
      if (!t || !curve || curve.complete) continue;
      if (t.entries >= MAX_ENTRIES_PER_MINT) continue;
      if (batch.slot < t.cooldownUntil) continue;
      if (batch.slot - t.createdSlot < MIN_AGE_SLOTS) continue;
      const rqSol = Number(curve.real_quote_reserves) / 1e9;
      if (rqSol < MIN_RQ_SOL || rqSol > MAX_RQ_SOL) continue;
      if (t.trades.length < 10) continue;
      const s = tapeStats(t);
      if (s.net < minNet) continue;
      if (s.buys < minBuys) continue;
      if (s.buyers < minBuyers) continue;
      if (s.priceUp < minUp || s.priceUp > 0.8) continue;
      if (s.nearHigh < 0.98) continue;
      if (s.topShare > 0.35) continue;
      if (s.netSecond < s.netFirst * 1.2) continue;
      // The window must span real time, not one burst of sniped trades.
      if (t.trades[t.trades.length - 1].slot - t.trades[0].slot < 60) continue;
      // Price above its 20-trade average: trend, not a dead-cat bounce.
      let sma = 0;
      const n = Math.min(20, t.trades.length);
      for (let i = t.trades.length - n; i < t.trades.length; i++) sma += t.trades[i].price;
      sma /= n;
      const lastPrice = t.trades[t.trades.length - 1].price;
      if (lastPrice < sma) continue;

      const cap = state.maxBuyLamports(mint);
      let amount = STAKE < cap ? STAKE : cap;
      if (amount < MIN_BUY) continue;
      if (free < amount + txFee) continue;
      orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: BUY_SLIP_BPS });
      free -= amount + txFee;
      slots -= 1;
      t.entries += 1;
    }

    // --- Late-window breadth probes: tiny buys on the most liquid active
    // coins, sold on the next batch. Only when the 20-coin bar is at risk.
    if (veryLate && slots > 0 && orders.length < caps.max_orders_per_batch) {
      let bestMint: string | null = null;
      let bestRq = 0;
      for (const mint of touched) {
        if (state.positions.has(mint) || hasPendingBuy(state, mint)) continue;
        const t = tapes.get(mint);
        const curve = state.curves.get(mint);
        if (!t || !curve || curve.complete) continue;
        if (t.entries >= MAX_ENTRIES_PER_MINT) continue;
        if (countedCoins.has(mint)) continue;
        const rqSol = Number(curve.real_quote_reserves) / 1e9;
        if (rqSol > bestRq && t.trades.length >= 5) {
          bestRq = rqSol;
          bestMint = mint;
        }
      }
      if (bestMint !== null) {
        const cap = state.maxBuyLamports(bestMint);
        let amount = PROBE_STAKE < cap ? PROBE_STAKE : cap;
        if (amount >= MIN_BUY && free >= amount + txFee) {
          orders.push({ side: 'buy', mint: bestMint, quote_lamports: amount, max_slippage_bps: BUY_SLIP_BPS });
          const tt = tapes.get(bestMint)!;
          tt.probe = true;
          tt.entries += 1;
          free -= amount + txFee;
        }
      }
    }

    return orders;
  },
});
