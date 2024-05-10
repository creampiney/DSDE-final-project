import streamlit as st
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration
from torch import cuda
import re

device = 'cuda' if cuda.is_available() else 'cpu'

model = T5ForConditionalGeneration.from_pretrained("./model_trained/").to(device)
tokenizer = T5Tokenizer.from_pretrained("./model_trained/")

# -------------------- Function for predicting -------------------------
def text_preprocessing(s):
    s = s.lower()
    # Change 't to 'not'
    s = re.sub(r"\'t", " not", s)
    # Remove @name
    s = re.sub(r'(@.*?)[\s]', ' ', s)
    # Isolate and remove punctuations except '?'
    s = re.sub(r'([\'\"\.\(\)\!\?\\\/\,])', r' \1 ', s)
    s = re.sub(r'[^\w\s\?]', ' ', s)
    # Remove some special characters
    s = re.sub(r'([\;\:\|•«\n])', ' ', s)
    # Remove trailing whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s

def predict(abstract):
    input_text = " ".join(abstract.split())
    input_text = text_preprocessing(input_text)

    source = tokenizer.batch_encode_plus(
        [input_text],
        max_length=512,
        pad_to_max_length=True,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    )

    ids = source["input_ids"].squeeze().to(device, dtype=torch.long).reshape((1, 512))
    mask = source["attention_mask"].squeeze().to(device, dtype=torch.long).reshape((1, 512))

    with torch.no_grad():
        generated_ids = model.generate(
            input_ids = ids,
            attention_mask = mask, 
            max_length=150, 
            num_beams=2,
            repetition_penalty=2.5, 
            length_penalty=1.0, 
            early_stopping=True
        )

        preds = [tokenizer.decode(g, skip_special_tokens=True, clean_up_tokenization_spaces=True) for g in generated_ids]

        prediction = " ".join(preds).capitalize()

        return prediction

    



st.header("Abstract to Title Generator")

abstract = st.text_area(
    "Fill your abstract",
    key="abstract",
)

if abstract:
    result = predict(abstract)
    st.subheader("Input Abstract")
    st.write(abstract)
    st.subheader("Generated Title")
    st.write(result)
