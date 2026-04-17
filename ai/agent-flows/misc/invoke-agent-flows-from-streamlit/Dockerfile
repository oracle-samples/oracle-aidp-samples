FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "aidp_streamlit_chat.py", "--server.port=8501", "--server.address=0.0.0.0"]
#CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
