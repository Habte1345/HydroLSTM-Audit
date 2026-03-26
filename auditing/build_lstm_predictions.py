from pathlib import Path
import pickle
import pandas as pd

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD


# ------------------------------------------------
# Paths
# ------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LSTM_FILE = Path(
r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\HydroAuditToolFrameowrk\runs\run_1303_1555_seed950317\lstm_seed950317.p"
)

OUTPUT_TTL = PROJECT_ROOT / "rdf" / "lstm_predictions.ttl"


# ------------------------------------------------
# Namespace
# ------------------------------------------------

HYDRO = Namespace("http://example.org/hydro/ontology#")


# ------------------------------------------------
# Load LSTM predictions
# ------------------------------------------------

def load_lstm_predictions():

    with open(LSTM_FILE, "rb") as f:
        results = pickle.load(f)

    df = pd.concat(results, names=["basin", "date"]).reset_index()

    print("Loaded predictions:", len(df))

    return df


# ------------------------------------------------
# Build Prediction KG
# ------------------------------------------------

def build_predictions():

    df = load_lstm_predictions()

    g = Graph()
    g.bind("hydro", HYDRO)

    total = 0

    for i, row in df.iterrows():

        basin = str(row["basin"])

        date = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")

        safe_date = date.replace("-", "")

        pred_uri = HYDRO[f"Prediction_{basin}_{safe_date}"]

        g.add((pred_uri, RDF.type, HYDRO.Prediction))

        g.add((pred_uri, HYDRO.forBasin, HYDRO[f"Basin_{basin}"]))

        g.add((pred_uri,
            HYDRO.hasTime,
            Literal(date, datatype=XSD.date)))

        g.add((pred_uri,
            HYDRO.hasStreamflow,
            Literal(float(row["qsim"]), datatype=XSD.float)))


        total += 1

    OUTPUT_TTL.parent.mkdir(parents=True, exist_ok=True)

    g.serialize(destination=str(OUTPUT_TTL), format="turtle")

    print("Saved prediction KG:", OUTPUT_TTL)
    print("Total predictions:", total)


# ------------------------------------------------
# Run
# ------------------------------------------------

if __name__ == "__main__":
    build_predictions()
