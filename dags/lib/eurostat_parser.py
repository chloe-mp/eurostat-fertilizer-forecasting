"""Eurostat JSON-stat 1.0 parser.

Pure parsing logic with no Airflow or network dependency,
extracted from the ETL DAG for testability.
"""


def parse_eurostat_json(json_data: dict) -> list[dict]:
    """Reconstruct flat records from Eurostat's JSON-stat 1.0 format.

    Eurostat returns multi-dimensional time series in a compressed
    indexed format: a flat dict of values keyed by a single integer,
    where each integer encodes a position in a multi-dimensional
    grid (geo × time × nutrient × ...).

    This function reverses that encoding into one record per cell.

    Args:
        json_data: Parsed JSON response from Eurostat's API.

    Returns:
        A list of dicts, one per non-null observation, with one
        key per dimension plus a "valeur" key for the measurement.
    """
    values = json_data["value"]
    dimensions = json_data["dimension"]
    id_list = json_data["id"]

    dim_map = {d: list(dimensions[d]["category"]["index"].keys()) for d in id_list}

    rows = []
    for idx_str, val in values.items():
        idx = int(idx_str)
        current_idx = idx
        row = {"valeur": val}
        for d in reversed(id_list):
            size = len(dim_map[d])
            row[d] = dim_map[d][current_idx % size]
            current_idx //= size
        rows.append(row)
    return rows
