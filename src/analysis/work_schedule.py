from src.analysis.config import SparkConnection

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


SPARK_URI = 'spark://127.0.0.1:7077'
APP_NAME = "work_schedule-viz"

# Make connection to local spark server
spark = SparkConnection(APP_NAME)


def save_graph(df, filename):
    fig, ax = plt.subplots(figsize=(20, 10))

    xx = df.index
    cumsum = np.zeros(df.shape[0])
    yticks = np.arange(0, 1.05, 0.05)
    index = 0
    fontcolor='green'
    colors = ['#D04B84', '#EDCEB0', '#768C24', '#D1DB56', '#5E5948']
    for (_, name), column in df.iteritems():

        column = column.rolling(4).mean()
        ax.fill_between(xx, cumsum, cumsum + column, label=name, color=colors[index], edgecolor='k', linewidth=2)
        cumsum += column
        index += 1
        
    handles, labels = ax.get_legend_handles_labels()

    ax.legend(handles, labels, loc='center', bbox_to_anchor=(0.5, -0.25), fontsize=20, ncol=5, borderaxespad=0)

    ax.set_yticks(yticks)
    ax.set_yticklabels(np.core.defchararray.add((yticks * 100).astype(int).astype(str), '%'), fontsize=20)

    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.tick_params(axis='x', rotation=45, labelsize=20)

    ax.grid(color='#FCB249', ls='--', lw=0.7)

    fig.tight_layout()
    ax.set_title('Work schedule', fontsize=30)

    fig.savefig(filename, dpi=300, bbox_inches='tight')


def main():
    df = spark.read()

    df = df.groupby(['published_at', 'schedule_name']) \
        .count() \
        .orderBy('published_at') \
        .toPandas() \
        .pivot('published_at', 'schedule_name') \
        .fillna(0)

    df = df.div(df.sum(axis=1), axis=0)

    df.rename(columns = {
        'Вахтовый метод': 'Fly-in fly-out',
        'Гибкий график': 'Flexible schedule',
        'Полный день': 'Full day',
        'Сменный график': 'Shift schedule',
        'Удаленная работа': 'Remote work',
    }, inplace = True)

    save_graph(df, 'images/work-schedule.png')


if __name__ == '__main__':
    main()
