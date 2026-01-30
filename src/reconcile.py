# src/reconcile.py
import logging
import pandas as pd

LOG = logging.getLogger(__name__)

def reconcile_raw_vs_ledger(
    raw_df: pd.DataFrame,
    ledger_df: pd.DataFrame,
    tolerance: float = 0.01,
) -> pd.DataFrame:
    """
    Reconcile the sum of successful raw transactions against the ledger.

    Both data‑frames are expected to contain the **same** amount column
    (``amount_clean`` in ``raw_df`` and ``amount`` in ``ledger_df``).  The
    function raises a ``ValueError`` if any daily difference exceeds the
    absolute ``tolerance``.
    """
    # Check if needed columns are present
    missing_raw = [c for c in ("status_clean", "amount_clean", "txn_date")
                   if c not in raw_df.columns]
    if missing_raw:
        raise ValueError(f"raw_df missing cols: {missing_raw}")
    
    missing_ledger = [c for c in ("amount", "txn_date")
                      if c not in ledger_df.columns]
    if missing_ledger:
        raise ValueError(f"ledger_df missing columns: {missing_ledger}")
    
    # Aggregate the *raw* side (only SUCCESS rows).
    raw_success = raw_df.loc[raw_df["status_clean"] == "SUCCESS", ["txn_date", "amount_clean"]]
    raw_agg = (
        raw_success.groupby("txn_date", observed=True)["amount_clean"]
        .sum()
        .reset_index()
        .rename(columns={"amount_clean": "raw_total"})
    )

    # Aggregate the *ledger* side.
    ledger_agg = (
        ledger_df.groupby("txn_date", observed=True)["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "ledger_total"})
    )

    # Compare
    recon_df = raw_agg.merge(ledger_agg, on="txn_date", how="outer", validate="one_to_one",).fillna(0)
    recon_df["diff"] = recon_df["raw_total"] - recon_df["ledger_total"]

    # Enforce tolerance
    failed = recon_df[recon_df["diff"].abs() > tolerance]
    if not failed.empty:
        LOG.error(
            "Reconciliation failed for %d date(s); sample:\n%s",
            len(failed),
            failed.head(10).to_string(index=False),
        )
        raise ValueError(f"Reconciliation FAILED:\n{failed}")

    LOG.info(
        "Reconciliation SUCCEEDED – %d dates compared, max diff = %.6f",
        len(recon_df),
        recon_df["diff"].abs().max(),
    )
    return recon_df.copy()
