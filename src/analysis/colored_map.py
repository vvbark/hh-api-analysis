from src.analysis.config import SparkConnection

import numpy as np
import cv2

from matplotlib import pyplot as plt
from PIL import Image


spark_uri = 'spark://127.0.0.1:7077'
app_name = 'application_name'

APP_NAME = "barkovskij-viz"

# Make connection to local spark server
spark = SparkConnection(app_name)

# Define Saint-Petersburg coordinates
min_lat, max_lat = 59.7498, 60.1357
min_lng, max_lng = 30.0002, 30.7645
coefficient = (max_lat - min_lat) / (max_lng - min_lng)
step = 30

# Define the map file
image_filepath = 'images/saint-petersburg-map.jpg'


def plot_and_save_colored_map(
        image: np.ndarray,
        density_matrix: np.ndarray,
        label: str,
        colormap: str,
        filepath: str,
):
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(image, alpha=1)
    cm = ax.imshow(density_matrix, cmap=colormap, alpha=0.4, interpolation='none', vmin=0)
    fig.colorbar(cm, label=label)
    ax.set_xticks([])
    ax.set_yticks([])
    plt.savefig(filepath, dpi=400)


def get_grid(step_ox: int = 10, step_oy: int = 10) -> tuple:
    """Get lat and lng values of target color map."""
    grid_ox = np.linspace(min_lat, max_lat, step_ox)
    grid_oy = np.linspace(min_lng, max_lng, step_oy)
    return grid_ox, grid_oy


def main():

    vacancies_df = spark.read()

    vacancies_df = vacancies_df.select([
        'id', 'salary_from', 'salary_to', 'address_lat', 'address_lng']) \
        .where(vacancies_df['address_lng'] > min_lng) \
        .where(vacancies_df['address_lat'] > min_lat) \
        .where(vacancies_df['address_lng'] < max_lng) \
        .where(vacancies_df['address_lat'] < max_lat) \
        .toPandas()

    grid_ox, grid_oy = get_grid(int(step * coefficient), step)
    vacancies_df['ind_ox'] = (vacancies_df['address_lat'].values[:, None] >= grid_ox).sum(axis=1) - 1
    vacancies_df['ind_oy'] = (vacancies_df['address_lng'].values[:, None] >= grid_oy).sum(axis=1) - 1

    mean_salary = vacancies_df.pivot_table(
        index=['ind_oy', 'ind_ox'],
        values='salary_from',
        aggfunc='mean',
    )
    count_vacancies = vacancies_df.pivot_table(
        index=['ind_oy', 'ind_ox'],
        values='id',
        aggfunc='count',
    )

    mean_density = np.zeros(np.broadcast(grid_ox, grid_oy[:, None]).shape, dtype=np.float32)
    count_density = np.zeros(np.broadcast(grid_ox, grid_oy[:, None]).shape, dtype=np.float32)

    for ind, item in mean_salary.iterrows():
        mean_density[ind] = item

    for ind, item in count_vacancies.iterrows():
        count_density[ind] = item

    image = np.array(Image.open(image_filepath))
    mean_density_resized = cv2.resize(mean_density, dsize=image.shape[:2], interpolation=cv2.INTER_CUBIC).T
    count_density_resized = cv2.resize(count_density, dsize=image.shape[:2], interpolation=cv2.INTER_CUBIC).T

    plot_and_save_colored_map(image, mean_density_resized, 'Mean Salary', 'Reds', 'images/MeanSalary.png')
    plot_and_save_colored_map(image, count_density_resized, 'Vacancies Count', 'Blues', 'images/VacanciesCount.png')

    spark.stop()

if __name__ == '__main__':
    main()
    