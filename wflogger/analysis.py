import pandas as pd
import matplotlib.pyplot as plt

from .credentials import creds, user_id
from .wflogger import DEFAULT_ITERATION, DEFAULT_FLAG


def _get_select_statement(workflow, tag=None, stage_number=None, stage=None, iteration=None,
                          user_id=user_id, hostname=None, comment="", flag=DEFAULT_FLAG):
    query = f"SELECT * FROM workflow_logs WHERE user_id = '{user_id}'"

    for str_arg in "workflow tag stage hostname comment".split():
        value = eval(str_arg)
        if value is not None:
            query += f" AND {str_arg} = '{value}'"

    for int_arg in "stage_number iteration flag".split():
        value = eval(int_arg)
        if value is not None:
            query += f" AND {int_arg} = {value}"

    return f"{query} ;"


def get_results(workflow, tag=None, stage_number=None, stage=None, iteration=None,
                hostname=None, comment="", flag=DEFAULT_FLAG):
    creds_dict = dict([item.split("=") for item in creds.split()])

    pg_url = "postgresql://{user}:{password}@{host}:{port}/{dbname}" \
             .format(**creds_dict)

    query = _get_select_statement(workflow, tag=tag, stage_number=stage_number, stage=stage, iteration=iteration,
                                  hostname=hostname, comment=comment, flag=flag)
    df = pd.read_sql(query, pg_url)
    
    # Add duration column
    return add_duration_column(df)


def rows_match(row1, row2, compare_columns=None):
    if compare_columns is None:
        compare_columns = ["user_id", "hostname", "workflow", "tag", "iteration"]

    return all(row1[compare_columns] == row2[compare_columns])


def add_duration_column(df, sort_by=None):
    if sort_by is None:
        sort_by = ["iteration", "stage_number"]
        
    previous_row = None
    new_rows = []
    
    for i, row in df.sort_values(sort_by).iterrows():

        if previous_row is not None and rows_match(previous_row, row):
            duration = (row["date_time"] - previous_row["date_time"]).total_seconds()
        else:
            duration = 0
            
        previous_row = row.copy()
        row["duration"] = duration
        new_rows.append(row)
        
    print(f"Converted {len(new_rows)} records.")
    return pd.DataFrame(new_rows, columns=list(df.columns) + ["duration"]).reset_index(drop=True)


def get_stage_numbers(df):
    return sorted(df.stage_number.unique())


def get_stage_labels(df):
    stages = []
    
    for i, row in df.iterrows():
        stage_number = row.stage_number
        stage = row.stage
        
        stage_label = f"{stage_number:02d}: {stage}"
        if stage_label in stages:
            continue
        
        stages.append(stage_label)

    return stages


def plot_stage_durations_by_iteration(df):
    fig, ax = plt.subplots(figsize=(9, 6))

    for key, grp in df.groupby(['iteration']):
        ax.scatter(grp['stage'], grp['duration'], label=key)

    #ax.legend()
    ax.set_ylabel("Duration (s)")
    ax.set_xlabel("Stage")

    stage_labels = get_stage_labels(df)
    ax.set_xticks(range(len(stage_labels)))
    ax.set_xticklabels(stage_labels)
    
    r1 = df.iloc[0]
    plt.title(f"{r1.workflow}: {r1.tag} - Iteration durations per stage")
    plt.show()


def plot_comparison_of_two_workflow_tags(df1, df2, stat="mean", yscale="linear"):
    fig, ax = plt.subplots(figsize=(12, 6))
    grp_1 = df1.groupby("stage")
    grp_2 = df2.groupby("stage")

    max_duration = max(grp_1["duration"].max())
    stage_labels = get_stage_labels(df1)

    r1 = df1.iloc[0]
    r2 = df2.iloc[0]
    
    ax.scatter(stage_labels, grp_1["duration"].agg(stat), label=f"{r1.workflow}: {r1.tag}")
    ax.scatter(stage_labels, grp_2["duration"].agg(stat), label=f"{r2.workflow}: {r2.tag}")

    n = len(stage_labels)
    ax.set_xticks(range(1, n+1))
    ax.set_xticklabels(stage_labels)
    ax.legend()
    
    plt.title(f"Comparing '{stat}' stage durations between: {r1.workflow}:" 
              f"{r1.tag} VS {r2.tag}")
    plt.yscale(yscale)
              
    ax.set_ylabel("Duration (s)")
    ax.set_xlabel("Stage")
       
    plt.show()


def plot_bar_chart_comparing_tags(df1, df2, legend_position=None):
    data_dict = {}
    stage_names = get_stage_labels(df1)
    tags = [_df.iloc[0].tag for _df in (df1, df2)]

    for i, sn in enumerate(sorted(df1["stage_number"].unique())):
        data_dict[stage_names[i]] = [_df[_df["stage_number"] == sn] \
                                     .groupby("stage_number")["duration"].agg("max").values[0] 
                                     for _df in (df1, df2)]

    index = tags
    plot_df = pd.DataFrame(data_dict, index=index)
    ax = plot_df.plot.bar(rot=0)
    
    if legend_position:
        ax.legend(loc=legend_position)

