import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from peft import PeftModel
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
load_dotenv()
import warnings
warnings.filterwarnings("ignore", message="Unexpected keyword arguments.*for class LoraConfig")
warnings.filterwarnings("ignore", category=UserWarning, module="peft.config")
def extract_location_disaster(sentence):
    llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash", # Changed from "gemini-1.5-flash"
    temperature=0.7,
    max_output_tokens=128,
    google_api_key=os.getenv("GEMINI_API_KEY") # Replace with your NEW key
)
    prompt = f"""
      Extract the following information from the sentence below:

      - Location: The place where the event occurred. Add more details if possible; if the context only gives the state or province, you should provide the country.
      - Disaster: The type of natural disaster (e.g., flood, earthquake, landslide, drought, storm, traffic accidents, etc.)

      If any information is missing, return "None".

      Sentence: "{sentence}"

      Respond **only** with a plain JSON object like this (without code block or markdown):

      {{
        "location": "...",
        "disaster": "..."
      }}
      """

    response = llm.invoke(prompt)
    return response.content
class DisasterPredictor:
    """Class to handle DistilBERT model loading and prediction"""
    
    def __init__(self, model_path="distilBERT"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        
        # Load your trained tokenizer from the model directory
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        
        # Load base model first - DistilBERT
        base_model = AutoModelForSequenceClassification.from_pretrained(
            "distilbert/distilbert-base-uncased",  # Use your base model here
            num_labels=2  # Binary classification for disaster/non-disaster
        )
        
        # Load your trained PEFT adapter
        self.model = PeftModel.from_pretrained(base_model, model_path)
        self.model.to(self.device)
        self.model.eval()
        
        
        print(f"Model device: {next(self.model.parameters()).device}")
        
        # Print adapter info
        
        # Test with a sample prediction to ensure everything works
        
    def predict_disaster(self, text):
        """Predict if text is about a disaster using your trained model"""
        try:
            if not text or text.strip() == "":
                return 0, 0.0
                
            # Tokenize input using your trained tokenizer
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512
            )
            
            # Move to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Get prediction from your trained model
            with torch.no_grad():
                outputs = self.model(**inputs)
                predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
                predicted_class = torch.argmax(predictions, dim=-1).item()
                confidence = predictions[0][predicted_class].item()
            
            # Return prediction (1 = disaster, 0 = not disaster)
            return int(predicted_class), float(confidence)
            
        except Exception as e:
            print(f"Error predicting for text: '{text[:50] if text else 'None'}...' Error: {str(e)}")
            return 0, 0.0  # Default to non-disaster with 0 confidence

 # Default to non-disaster with 0 confidence