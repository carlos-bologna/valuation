# Stock Price Prediction Using Artificial Intelligence

This repository contains the code and resources for the project "Stock Price Prediction Using Artificial Intelligence: An Integrated Approach with Facebook Prophet and RNN LSTM." This project focuses on predicting stock prices using time series analysis and deep learning models.

## Requirements

- Python 3.x
- Jupyter Notebook
- Dev Container

## Installation

To set up the project using a Dev Container, follow these steps:

### Prerequisites

- Install [Docker](https://www.docker.com/get-started) on your machine.
- Install [Visual Studio Code](https://code.visualstudio.com/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).

### Setting Up the Dev Container

1. **Clone the repository**:
   ```bash
   git clone https://github.com/carlos-bologna/valuation.git
   cd valuation
   ```

2. **Open the repository in VS Code**:
   ```bash
   code .
   ```

3. **Open the project in a Dev Container**:
   - Once in VS Code, a prompt may appear asking if you want to reopen the project in a Dev Container. Click "Reopen in Container".
   - If the prompt does not appear, open the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P` on macOS) and select:
     ```
     >Dev Containers: Reopen in Container
     ```

4. **Wait for the container to build**: The initial build may take a few minutes as it installs all necessary dependencies and sets up the environment inside the container.

### Using the Dev Container

- Once the container is set up, you can use the integrated terminal in VS Code to run commands.
- The required libraries and tools are pre-installed, so you can start running the notebooks and scripts without additional setup.

### Running Jupyter Notebooks

To start Jupyter Notebook within the Dev Container, run:
```bash
jupyter notebook
```

Now, you can access the notebooks in your browser and start using the project.


## Configuration

Before running the project, you need to set up the configuration file. The config file is located in the main folder and is named `config.yaml`.

### Setting up config.yaml

1. **Locate the file**: Find `config.yaml` in the root directory of the project.

2. **Edit the file**: Open `config.yaml` in a text editor and modify the desired parameters.

3. **Save the file**: After making your changes, save the `config.yaml` file.

Make sure to adjust these parameters according to your specific requirements and file structure.

## Usage

1. **ETL (Extract, Transform and Load)**: Ensure you have the historical stock data for the target company (e.g., VALE3) in a CSV file.

   ```bash
   $ python src/stock_price/extraction_from_yahoo.py
   $ python src/stock_price/bronze.py
   $ python src/stock_price/silver.py
   $ python src/stock_price/gold_stock_price_labeled.py
   ```

2. **Data Preparation**: Use Jupyter Notebook to run the provided scripts for data preprocessing.

   ```bash
   jupyter notebook
   ```

   Run the notebook **preprocess.ipynb**

3. **Model Training**: Use Jupyter Notebook to run the provided scripts for model training and forecasting.

   ```bash
   jupyter notebook
   ```

   Run the notebook **train.ipynb**

## Contributing

Contributions are welcome! If you would like to enhance the project, please fork the repository and create a pull request with a clear description of the changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
