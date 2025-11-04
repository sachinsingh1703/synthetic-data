import google.generativeai as genai

    # Configure your API key
genai.configure(api_key="AIzaSyDPHB2oVhi7j14iVkWLtRU8qYhTw7TZWEY")

    # List available models
for m in genai.list_models():
    if "generateContent" in m.supported_generation_methods:
        print(m.name)