import pandas as pd
from pathlib import Path


def check_required_columns(df, required_cols):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def audit_flow_continuity(
    df: pd.DataFrame,
    reach_id_col="reach_id",
    predicted_q_col="predicted_q",
    upstream_sum_col="upstream_q_sum",
    local_inflow_col="local_inflow",
    tolerance=1e-6,
):
    """
    Checks:
        predicted_q ≈ upstream_q_sum + local_inflow

    Returns DataFrame with:
        expected_q
        residual
        continuity_ok
    """

    check_required_columns(
        df,
        [reach_id_col, predicted_q_col, upstream_sum_col, local_inflow_col],
    )

    out = df.copy()

    out["expected_q"] = out[upstream_sum_col] + out[local_inflow_col]
    out["residual"] = out[predicted_q_col] - out["expected_q"]
    out["continuity_ok"] = out["residual"].abs() <= tolerance

    return out


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "reach_id": [1, 2, 3],
            "predicted_q": [10.0, 15.0, 9.5],
            "upstream_q_sum": [8.0, 12.0, 7.0],
            "local_inflow": [2.0, 3.0, 2.5],
        }
    )

    result = audit_flow_continuity(sample, tolerance=0.01)
    print(result)