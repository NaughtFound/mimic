# mimic

A modular PyTorch dataset library for working with [MIMIC-CXR-JPG](https://physionet.org/content/mimic-cxr-jpg/) and [MIMIC-IV](https://physionet.org/content/mimiciv/) datasets.

This library provides ready-to-use dataset classes and a clean `BaseDataset` abstraction to help researchers quickly build and experiment with MIMIC-derived datasets.

---

## ✨ Features

- 🏥 Built-in support for **MIMIC-CXR-JPG** and **MIMIC-IV**
- 🧩 Define your own dataset in seconds using `BaseDataset`
- ⚡ Fully compatible with PyTorch’s `DataLoader`
- 🛠️ Helper functions for filtering, processing, and batching
- 📓 Example notebooks for rapid exploration

---

## 🐍 Requirements

- Python ≥ **3.10**
- PyTorch ≥ **2.5**
- Access to MIMIC-CXR-JPG and/or MIMIC-IV via PhysioNet

---

## 📦 Installation

Install directly via pip:

```bash
pip install git+https://github.com/naughtFound/mimic.git
```

---

## 📁 Project Structure

```
mimic/
├── datasets/         # Core Dataset classes (BaseDataset, CXR, IV)
├── utils/            # Shared utilities and helpers
notebooks/
├── cxr.ipynb         # MIMIC-CXR visualization and exploration
├── iv.ipynb          # MIMIC-IV usage demo
requirements.txt      # Dependency list
setup.py              # Install script
pyproject.toml        # Build metadata
```

---

## 🧱 Creating Custom Datasets

To define your own dataset, subclass `BaseDataset` and implement two things:

- `_files()`: a list of files to download from [PhysioNet](https://physionet.org).
- `collate_fn()`: how to batch items together for the `DataLoader`

That's it — no need to redefine PyTorch boilerplate.

```python
from mimic.datasets import BaseDataset

class MyDataset(BaseDataset):
    def _files(self) -> dict:
        # Prepare your list of files or samples
        return {...}

    def collate_fn(self, batch:list[int]):
        # Define how samples should be combined into a batch
        return ...
```

---

## 📄 License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Have ideas, improvements, or bug fixes?
Open an issue or submit a pull request — contributions are welcome!

---

## ⚠️ Data Access

You must be credentialed and approved via [PhysioNet](https://physionet.org) to access:

- [MIMIC-CXR-JPG](https://physionet.org/content/mimic-cxr-jpg/)
- [MIMIC-IV](https://physionet.org/content/mimiciv/)

See the respective pages for details on requesting access.
