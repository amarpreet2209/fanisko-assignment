In Google Cloud Storage bucket, create two directories - code and raw-data. In raw-data, there needs to be two directories - file_layout containing file_layout.json and input_datasets containing member_dataset.txt.


In code, put app.py and config.json. Then run app.py using Google Cloud CLI. It should create two directories - processed_data and logs. Processed_data contains data for bronze_layer, silver_layer and gold_layer. Gold layer is copied to mongodb.