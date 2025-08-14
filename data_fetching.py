import pandas as pd
import numpy as np
import json
import os


def tq_trades(
    client,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    coin_filter: str | None = None,
    strategy_filter: str | None = None,
    address_filter: str | None = None,
) -> pd.DataFrame:
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
                    *
                    FROM tq.tq_view(start_time='{start_time}', end_time='{end_time}')
                    WHERE time > '{start_time}'
            """
    if coin_filter:
        query += f" AND coin = '{coin_filter}'"
    if strategy_filter:
        query += f" AND strategy = '{strategy_filter}'"
    if address_filter:
        query += f" AND address = '{address_filter}'"
    query += " ORDER BY time LIMIT 500000"

    df = client.query_df(query)
    df["time"] = pd.to_datetime(df["time"])
    df["px"] = df["px"].astype(float)
    df["fee"] = df["fee"].astype(float)
    df["sz"] = df["sz"].astype(float)
    df["start_position"] = df["start_position"].astype(float)
    df["closed_pnl"] = df["closed_pnl"].astype(float)

    df = df.sort_values(by="time")

    return df


def minute_tobs(
    client, start_time: pd.Timestamp, end_time: pd.Timestamp, coins: list[str]
) -> pd.DataFrame:
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
                    toStartOfMinute(time) as time,
                    last_value(bid_px) as bid_px,
                    last_value(ask_px) as ask_px,
                    friendly_coin
                    FROM hyperliquid.tobs
                    WHERE time > '{start_time}'
                    AND time < '{end_time}'
                    AND friendly_coin IN ( '{"', '".join(coins)}' )
                    GROUP BY time, friendly_coin
                    ORDER BY time
                    LIMIT 5000000
                    """

    df = client.query_df(query)
    df["time"] = pd.to_datetime(df["time"])
    df["bid_px"] = df["bid_px"].astype(float)
    df["ask_px"] = df["ask_px"].astype(float)

    df = df.sort_values(by="time").drop_duplicates(subset=["time", "friendly_coin"])
    return df


def order_metas(
    client, start_time: pd.Timestamp, end_time: pd.Timestamp, strategies: list[str]
) -> pd.DataFrame:
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
                    strategy_name,
                    time,
                    cloid
                FROM strategy.order_meta
                    WHERE time > '{start_time}'
                    AND time < '{end_time}'
                    AND strategy_name IN ( '{"', '".join(strategies)}' )
                    ORDER BY time
                    LIMIT 5000000
                    """

    df = client.query_df(query)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(by="time")
    return df


def tobs(
    client, start_time: pd.Timestamp, end_time: pd.Timestamp, friendly_coins: list[str]
) -> pd.DataFrame:
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
            friendly_coin,
            capture_time,
            time,
            bid_px,
            ask_px
        FROM hyperliquid.tobs
            WHERE time > '{start_time}'
            AND time < '{end_time}'
            AND tobs.friendly_coin IN ( '{"', '".join(friendly_coins)}' )
            ORDER BY time
            LIMIT 1 by (friendly_coin, time)
            LIMIT 5000000;"""

    df = client.query_df(query)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(by="time")
    return df


def bbos(
    client,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    friendly_coins: list[str],
    lags: list[str] | None = None,
) -> pd.DataFrame:
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
            friendly_coin,
            capture_time,
            time,
            bid_px,
            ask_px
        FROM hyperliquid.bbo
            WHERE time > '{start_time}'
            AND time < '{end_time}'
            AND bbo.friendly_coin IN ( '{"', '".join(friendly_coins)}' )
            ORDER BY time DESC
            LIMIT 1 by (friendly_coin, time)
            LIMIT 5000000;"""

    df = client.query_df(query)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(by="time")
    if lags:
        df = add_lags(df, lags)
    return df


def trades(
    client,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    friendly_coins: list[str] | None = None,
    address: str | None = None,
) -> pd.DataFrame:
    start_time_s = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_s = end_time.strftime("%Y-%m-%d %H:%M:%S")

    where = [
        f"time > '{start_time_s}'",
        f"time < '{end_time_s}'",
    ]

    if friendly_coins:
        coins_sql = ", ".join("'" + c.replace("'", "''") + "'" for c in friendly_coins)
        where.append(f"trades.friendly_coin IN ({coins_sql})")

    if address:
        addr_sql = address.replace("'", "''")
        where.append(
            f"(trades.buy_user = '{addr_sql}' OR trades.sell_user = '{addr_sql}')"
        )

    where_sql = " AND ".join(where)

    query = f"""
        SELECT
            friendly_coin,
            capture_time,
            time,
            side,
            toFloat64(px) AS px,
            toFloat64(sz) AS sz,
            tid,
            buy_user,
            sell_user
        FROM hyperliquid.trades
        WHERE {where_sql}
        ORDER BY time
        LIMIT 1 BY (friendly_coin, time, tid)
        LIMIT 7000000;
    """

    df = client.query_df(query)
    df["sign"] = df["side"].apply(lambda x: 1 if x == "B" else -1)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(by="time")
    return df


def twap_trades(
    client,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    friendly_coins: list[str] | None = None,
    address: str | None = None,
) -> pd.DataFrame:
    start_time_s = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_s = end_time.strftime("%Y-%m-%d %H:%M:%S")

    where = [
        f"time > '{start_time_s}'",
        f"time < '{end_time_s}'",
        "hash = '0x0000000000000000000000000000000000000000000000000000000000000000'",
    ]

    if friendly_coins:
        coins_sql = ", ".join("'" + c.replace("'", "''") + "'" for c in friendly_coins)
        where.append(f"trades.friendly_coin IN ({coins_sql})")

    if address:
        addr_sql = address.replace("'", "''")
        where.append(
            f"(trades.buy_user = '{addr_sql}' OR trades.sell_user = '{addr_sql}')"
        )

    where_sql = " AND ".join(where)

    query = f"""
        SELECT
            friendly_coin,
            capture_time,
            time,
            side,
            hash,
            toFloat64(px) AS px,
            toFloat64(sz) AS sz,
            tid,
            buy_user,
            sell_user
        FROM hyperliquid.trades
        WHERE {where_sql}
        ORDER BY time
        LIMIT 1 BY (friendly_coin, time, tid)
        LIMIT 7000000;
    """

    df = client.query_df(query)
    df["sign"] = df["side"].apply(lambda x: 1 if x == "B" else -1)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(by="time")
    return df


def add_lags(bbos_df, lags):
    bbos_df["mid"] = (
        bbos_df["bid_px"].astype(float) + bbos_df["ask_px"].astype(float)
    ) / 2
    bbos_df = (
        bbos_df.reset_index().set_index(["friendly_coin", "capture_time"]).sort_index()
    )
    coins = bbos_df.index.get_level_values(0).unique()

    for lag in lags:
        for coin in coins:
            coin_df = bbos_df.loc[coin][["mid"]]
            bbos_df.loc[coin, f"abs_log_bps_ret_{lag}"] = np.abs(
                (
                    np.log(
                        coin_df.asof(coin_df.index + pd.Timedelta(lag)).mid.values
                        / coin_df.mid
                    )
                    * 10000
                ).values
            )
            bbos_df.loc[coin, f"bps_ret_{lag}"] = (
                coin_df.asof(coin_df.index + pd.Timedelta(lag)).mid.values / coin_df.mid
                - 1
            ).values * 10000

        bbos_df[f"abs_bps_ret_{lag}"] = bbos_df[f"bps_ret_{lag}"].abs()

    bbos_df = bbos_df.reset_index()
    return bbos_df


def backtest_fills(
    directory: str, coin_filter: list[str] | None = None, max_rows: int = 50000
) -> pd.DataFrame:
    file_path = os.path.join(directory, "fills.jsonl")
    fills_list = []
    row = 0
    with open(file_path, "r") as f:
        for line in f:
            if row >= max_rows:
                break
            row += 1
            data = json.loads(line.strip())
            user = data.get("user")
            for fill in data.get("fills", []):
                fill_dict = fill.copy()
                fill_dict["user"] = user
                fills_list.append(fill_dict)
    df = pd.DataFrame(fills_list)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["oid"] = df["oid"].astype("int64")
    df["tid"] = df["tid"].astype("int64")
    df["px"] = df["px"].astype(float)
    df["sz"] = df["sz"].astype(float)
    df["startPosition"] = df["startPosition"].astype(float)
    df["closedPnl"] = df["closedPnl"].astype(float)
    df["fee"] = df["fee"].astype(float)
    if coin_filter:
        df = df[df["coin"].isin(coin_filter)]
    df = df.sort_values(by="time")
    return df


def backtest_orders(
    directory: str,
    coin_filter: list[str] | None = None,
    include_meta: bool = False,
    max_rows: int = 50000,
) -> pd.DataFrame:
    orders_file_path = os.path.join(directory, "orders.jsonl")
    order_meta_file_path = os.path.join(directory, "order_meta.jsonl")
    cloids = set()

    # Read orders
    orders_list = []
    row = 0
    with open(orders_file_path, "r") as f:
        for line in f:
            if row >= max_rows:
                break
            row += 1
            data = json.loads(line.strip())
            order_inner = data["order"]["order"]
            order_dict = {
                "address": data["address"],
                "cloid": order_inner["cloid"],
                "coin": order_inner["coin"],
                "limit_px": float(order_inner["limitPx"]),
                "oid": int(order_inner["oid"]),
                "orig_sz": float(order_inner["origSz"]),
                "side": order_inner["side"],
                "sz": float(order_inner["sz"]),
                "timestamp": int(order_inner["timestamp"]),
                "status": data["order"]["status"],
                "status_timestamp": int(data["order"]["statusTimestamp"]),
            }
            cloids.add(order_dict["cloid"])
            orders_list.append(order_dict)

    if not orders_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(orders_list)
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["status_time"] = pd.to_datetime(df["status_timestamp"], unit="ms")
    
    if include_meta:
        meta_list = []
        row = 0
        with open(order_meta_file_path, "r") as f:
            for line in f:
                data = json.loads(line.strip())
                if data['cloid'] not in cloids:
                    continue
                if row >= max_rows:
                    break
                row += 1
                float_dict = {k: v for k, v in data.get("float_values", [])}
                timestamp_dict = {k: v for k, v in data.get("timestamp_values", [])}
                string_dict = {k: v for k, v in data.get("string_values", [])}
                data.update(float_dict)
                data.update(timestamp_dict)
                data.update(string_dict)
                del data["float_values"]
                del data["timestamp_values"]
                del data["string_values"]
                meta_list.append(data)
        if meta_list:
            df_meta = pd.DataFrame(meta_list)
            df_meta["meta_time"] = pd.to_datetime(df_meta["time"], format="ISO8601")
            df = pd.merge(df, df_meta, on="cloid", how="left", suffixes=("", "_meta"))
        
    if coin_filter:
        df = df[df["coin"].isin(coin_filter)]
    df = df.sort_values(by="time")
    return df


def backtest_strategy_info(
    directory: str, strategy_name_filter: list[str] | None = None, max_rows: int = 50000
) -> pd.DataFrame:
    file_path = os.path.join(directory, "strategy_info.jsonl")
    info_list = []
    row = 0
    with open(file_path, "r") as f:
        for line in f:
            data = json.loads(line.strip())
            if len(data) != 2:
                continue
            info_dict, strategy_name = data
            if not info_dict:
                continue
            strategy_type = list(info_dict.keys())[0]
            if strategy_name_filter is not None:
                if strategy_name not in strategy_name_filter:
                    continue
            inner = info_dict[strategy_type].copy()
            inner["strategy_type"] = strategy_type
            inner["strategy_name"] = strategy_name
            info_list.append(inner)
            row += 1
            if row >= max_rows:
                break

    df = pd.DataFrame(info_list)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"], format="ISO8601")
    df = df.sort_values(by="time")
    return df


def backtest_theos(
    directory: str, coin_filter: list[str] | None = None, max_rows: int = 50000
) -> pd.DataFrame:
    file_path = os.path.join(directory, "theo.jsonl")
    theos_list = []
    row = 0
    with open(file_path, "r") as f:
        for line in f:
            data = json.loads(line.strip())
            if coin_filter and data["friendly_coin"] not in coin_filter:
                continue
            row += 1
            if row >= max_rows:
                break
            float_dict = {k: v for k, v in data.get("float_values", [])}
            data.update(float_dict)
            del data["float_values"]
            theos_list.append(data)
    df = pd.DataFrame(theos_list)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"], unit="ns")
    if coin_filter:
        df = df[df["friendly_coin"].isin(coin_filter)]
    df = df.sort_values(by="time")
    return df


def backtest_ws_requests(directory: str, max_rows: int = 50000) -> pd.DataFrame:
    file_path = os.path.join(directory, "ws_request.jsonl")
    requests_list = []
    row = 0
    with open(file_path, "r") as f:
        for line in f:
            if row >= max_rows:
                break
            row += 1
            data = json.loads(line.strip())
            requests_list.append(data)
    df = pd.DataFrame(requests_list)
    if df.empty:
        return df
    if "response_capture_time" in df.columns:
        df["response_capture_time"] = pd.to_datetime(
            df["response_capture_time"], unit="ns"
        )
        df = df.sort_values(by="response_capture_time")
    elif "submit_time" in df.columns:
        df["submit_time"] = pd.to_datetime(df["submit_time"], unit="ns")
        df = df.sort_values(by="submit_time")
    if "response" in df.columns:
        df["response"] = df["response"].apply(json.loads)
    return df
