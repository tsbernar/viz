from bokeh.models import ColumnDataSource
from bokeh.palettes import Category20


def add_theo_and_features_to_fig(fig, theo_df, feature_names):
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
    theo_df = theo_df.sort_values(by="time")
    theo_df["next_time"] = theo_df["time"].shift(-1)

    theo_data_source = ColumnDataSource(data=theo_df)

    colors = Category20[20]

    fig.step(
        x="time",
        y="mid",
        color="black",
        legend_label="mid",
        mode="after",
        source=theo_data_source,
    )
    fig.step(
        x="time",
        y="theo",
        color="red",
        legend_label="theo",
        mode="after",
        source=theo_data_source,
    )

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

    fig.legend.location = "top_left"
    fig.legend.click_policy = "hide"

    return fig
