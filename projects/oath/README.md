# Oath Analysis Project

Data analysis project using Python.

## Project Structure

```
oath/
├── data/
│   ├── raw/          # Original, immutable data
│   ├── processed/    # Cleaned and transformed data
│   └── external/     # External data sources
├── notebooks/        # Jupyter notebooks for exploration
├── src/             # Source code and modules
├── output/          # Generated outputs, figures, reports
├── tests/           # Unit tests
└── requirements.txt # Python dependencies
```

## Setup

### 1. Activate Virtual Environment

```bash
source venv/bin/activate
```

### 2. Install Dependencies

Dependencies are already installed, but to reinstall or update:

```bash
pip install -r requirements.txt
```

### 3. Deactivate Virtual Environment

When finished working:

```bash
deactivate
```

## Installed Packages

- **Data Analysis**: pandas, numpy, scipy
- **Visualization**: matplotlib, seaborn, plotly
- **Jupyter**: jupyter, ipykernel, notebook
- **Machine Learning**: scikit-learn, statsmodels
- **Development**: pytest, black, flake8

## Usage

### Running Jupyter Notebook

```bash
source venv/bin/activate
jupyter notebook
```

### Running Python Scripts

```bash
source venv/bin/activate
python src/your_script.py
```

## Development

### Code Formatting

```bash
black src/
```

### Linting

```bash
flake8 src/
```

### Running Tests

```bash
pytest tests/
```
