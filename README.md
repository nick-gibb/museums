# Museums ETL and Analysis

This repository contains a data pipeline and analysis of museum attendance data. The data is extracted from Wikipedia, transformed, and loaded into a SQLite database. The analysis includes fitting a linear regression model to the data and visualizing the results.

## Analysis

The analysis is performed using a Jupyter Notebook (analysis.ipynb). The notebook contains a linear regression model that predicts the number of museum visitors based on the population of the city in which the museum is located. The results are also visualized for better understanding.

## Schema

Tables in Database Schema:

`museum`:

- name (text; primary key)
- city (text)
- visitors_per_year (integer)
- year_reported (integer)

`city`:

- name (primary key)
- population (integer)

TODO: This schema should be further normalized by adding a museum_visit table that contains the number of visitors per year for each museum.

## Requirements

- Docker (for running the ETL and analysis scripts)

- API Key from api.api-ninjas.com (free), required for obtaining city populations.

## Usage

1. Clone the repository.

2. Build the Docker image by running the following command:

```bash
docker build -t museums .
```

3. Run the Docker container. You can use a volume for museums.db to persist the database between runs. You can also enter the container in interactive mode to run the ETL script and the analysis notebook.

```bash
docker run -it --rm -v $(pwd)/museums.db:/museums/museums.db museums
```

Or for local development, you can make the entire working directory a volume:

```bash
docker run -it --rm -v $(pwd):/museums museums
```

4. Run the ETL script:

```bash
python etl.py
```

You have the option to skip API calls by using the `--use-cache` argument and clear the database before running the ETL script using the `--clear` argument.

5. Launch the analysis notebook:

```bash
jupyter notebook analysis.ipynb
```

## Tests

Tests for the etl.py script are located in etl_test.py. To run the tests, you can use the following command:

```bash
pytest etl_test.py
```

## Formatting

This project uses the black library for formatting code. To format the code, you can use the following command:

```bash
black etl.py etl_test.py
```

## Linting

This project uses pylint for linting. To run linting, you can use the following command:

```bash
pylint etl.py etl_test.py
```

## License

This project is licensed under the MIT License.
