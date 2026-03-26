# HydroKG

HydroKG is a hydrologic knowledge graph framework designed to audit machine learning based hydrologic predictions using physical laws.

The framework integrates hydrologic datasets, river network topology, and physical constraints to evaluate LSTM-based hydrologic models.

---

## Key Features

• Hydrologic Knowledge Graph construction  
• Basin topology extraction from NHDPlus  
• Integration of LSTM predictions  
• Physical law auditing (mass balance, Budyko constraints)  
• Interactive KG visualization  

---

## Framework Architecture

HydroKG consists of three main layers:

1. Schema layer (T-Box)
2. Basin knowledge layer (A-Box)
3. Hydrologic auditing engine

---

## Installation

```bash
git clone https://github.com/Habte1345/HydroKG.git
cd HydroKG
pip install -r requirements.txt