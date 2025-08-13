from bokeh.models import ColumnDataSource, HoverTool
from bokeh.palettes import Category20
import numpy as np


def add_fills_to_fig(fig, fills_df):
    fills_df = fills_df.copy()

    fills_df["color"] = fills_df["side"].map({"B": "blue", "A": "red"})
    fills_df["angle"] = fills_df["side"].map({"B": 0, "A": 3.14})
    fills_df["plot_size"] = np.clip(
        np.sqrt(fills_df["sz"] * fills_df["px"]) / np.sqrt(1000) * 10, 3, 20
    )
    fills_source = ColumnDataSource(fills_df)

    renderer = fig.scatter(
        x="time",
        y="px",
        source=fills_source,
        color="color",
        legend_label="fills",
        marker="triangle",
        angle="angle",
        size="plot_size",
    )

    hover = HoverTool(
        renderers=[renderer],
        tooltips=[
            ("time", "@time{%F %T}"),  # formatted datetime
            ("px", "@px{0.0000}"),
            ("sz", "@sz{0.00}"),
            ("side", "@side"),
            ("strategy_name", "@strategy_name"),
        ],
        formatters={"@time": "datetime"},
    )
    fig.add_tools(hover)

    return fig


def add_theo_with_lean_to_fig(fig, info_df):
    info_df = info_df.copy()
    info_df["theo_with_lean"] = info_df["theo"] * (1 + info_df["lean_bps"] / 10000)
    info_df = info_df.sort_values(by="time")[
        ["time", "theo_with_lean", "strategy_name", "theo"]
    ]
    colors = Category20[20]

    for strategy_name, color in zip(info_df.strategy_name.unique(), colors):
        info_df_strategy = info_df[info_df["strategy_name"] == strategy_name]
        info_data_source = ColumnDataSource(info_df_strategy)

        fig.step(
            x="time",
            y="theo_with_lean",
            color=color,
            legend_label=f"{strategy_name} theo + lean",
            mode="after",
            source=info_data_source,
        )

    return fig


def add_theo_and_features_to_fig(
    fig, theo_df, feature_names, include_features=True, spot_label=False
):
    theo_df = theo_df.copy()

    theo_df = theo_df.sort_values(by="time")
    theo_df["next_time"] = theo_df["time"].shift(-1)

    if include_features:
        colors = Category20[20]

        for feat in feature_names:
            theo_df[f"{feat}+"] = theo_df[feat].clip(lower=0)
            theo_df[f"{feat}-"] = theo_df[feat].clip(upper=0)

        theo_df["pos_cum_0"] = theo_df["mid"]
        for i in range(1, len(feature_names) + 1):
            theo_df[f"pos_cum_{i}"] = (
                theo_df[f"pos_cum_{i - 1}"] + theo_df[f"{feature_names[i - 1]}+"]
            )

        theo_df["neg_cum_0"] = theo_df["mid"]
        for i in range(1, len(feature_names) + 1):
            theo_df[f"neg_cum_{i}"] = (
                theo_df[f"neg_cum_{i - 1}"] + theo_df[f"{feature_names[i - 1]}-"]
            )

        # Compute lean and theo + lean
        theo_df["lean"] = theo_df[feature_names].sum(axis=1)

    theo_data_source = ColumnDataSource(data=theo_df)

    fig.step(
        x="time",
        y="mid",
        color="black" if not spot_label else "brown",
        legend_label="mid" if not spot_label else "spot mid",
        mode="after",
        source=theo_data_source,
    )
    fig.step(
        x="time",
        y="theo",
        color="red" if not spot_label else "blue",
        legend_label="theo" if not spot_label else "spot theo",
        mode="after",
        source=theo_data_source,
    )

    if include_features:
        # Positive components (stacked on top of mid)
        for i, feat in enumerate(feature_names):
            bottom_col = f"pos_cum_{i}"
            top_col = f"pos_cum_{i + 1}"
            fig.quad(
                left="time",
                right="next_time",
                bottom=bottom_col,
                top=top_col,
                color=colors[i],
                fill_alpha=0.3,
                line_alpha=0,
                legend_label=feat,
                source=theo_data_source,
            )

        # Negative components (stacked below mid)
        for i, feat in enumerate(feature_names):
            bottom_col = f"neg_cum_{i + 1}"
            top_col = f"neg_cum_{i}"
            fig.quad(
                left="time",
                right="next_time",
                bottom=bottom_col,
                top=top_col,
                color=colors[i],
                fill_alpha=0.3,
                line_alpha=0,
                legend_label=feat,
                source=theo_data_source,
            )

    return fig
