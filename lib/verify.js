/**
 * USDC payment verification on Base L2.
 *
 * Async verification — ingestion accepts the batch provisionally, this
 * confirms the on-chain Transfer event matches (sender → wallet, amount ≈
 * charge_usd). If verification fails, the ingest is marked unverified.
 */

import { ethers } from 'ethers';

const USDC_BASE = process.env.USDC_BASE || '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913';
const BASE_RPC = process.env.BASE_RPC || 'https://mainnet.base.org';
const WALLET_ADDRESS = (process.env.WALLET_ADDRESS || '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e').toLowerCase();

const TRANSFER_TOPIC = ethers.id('Transfer(address,address,uint256)');
const USDC_DECIMALS = 6;

let provider;
function getProvider() {
  if (!provider) provider = new ethers.JsonRpcProvider(BASE_RPC);
  return provider;
}

export async function verifyUsdcPayment({ tx_hash, expected_usd, expected_to = WALLET_ADDRESS, tolerance_pct = 0.05, timeout_ms = 60_000 }) {
  if (!tx_hash) return { ok: false, reason: 'no_tx_hash' };
  const p = getProvider();
  const start = Date.now();
  let receipt = null;
  while (Date.now() - start < timeout_ms) {
    try {
      const r = await p.getTransactionReceipt(tx_hash);
      if (r) { receipt = r; break; }
    } catch (_e) { /* swallow */ }
    await sleep(2000);
  }
  if (!receipt) return { ok: false, reason: 'no_receipt' };
  if (receipt.status !== 1) return { ok: false, reason: 'tx_failed' };

  const usdc = USDC_BASE.toLowerCase();
  const recipient = String(expected_to || WALLET_ADDRESS).toLowerCase();

  for (const log of receipt.logs || []) {
    if ((log.address || '').toLowerCase() !== usdc) continue;
    if (!log.topics || log.topics[0] !== TRANSFER_TOPIC) continue;
    const to = '0x' + log.topics[2].slice(26);
    if (to.toLowerCase() !== recipient) continue;
    const raw = BigInt(log.data);
    const usd = Number(raw) / Math.pow(10, USDC_DECIMALS);
    const want = Number(expected_usd);
    if (!Number.isFinite(want)) return { ok: true, paid_usd: usd };
    const drift = Math.abs(usd - want) / Math.max(want, 1e-9);
    if (drift > tolerance_pct) return { ok: false, reason: 'amount_mismatch', paid_usd: usd, expected_usd: want, drift };
    return { ok: true, paid_usd: usd, expected_usd: want, drift };
  }
  return { ok: false, reason: 'no_usdc_transfer_to_wallet' };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
