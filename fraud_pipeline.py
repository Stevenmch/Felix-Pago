import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import json


# --- Helper functions ---
def parse_json(line):
    # Converts the JSON message into a Python dictionary
    data = json.loads(line)
    print(f"Processed line: {data}")
    return {
        'transaction_id': data['transaction_id'],
        'user_id': data['user_id'],
        'amount': float(data['amount']),
        'origin_country': data['origin_country'],
        'destination_country': data['destination_country'],
        'payment_method': data['payment_method'],
        'timestamp': data['timestamp'],
        'card_number': data['card_number'],
        'phone_number': data['phone_number'],
    }

def to_unix_timestamp(timestamp_str):
    try:
        dt = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M")
        return int(dt.timestamp())
    except Exception as e:
        print(f"⚠️ Error converting timestamp: {timestamp_str} -> {e}")
        return 0

# --- Fraud detection rules ---
def flag_suspicious_country_switch(transactions):
    transactions = sorted(transactions, key=lambda x: to_unix_timestamp(x['timestamp']))
    suspicious = []
    for i in range(1, len(transactions)):
        t1 = transactions[i - 1]
        t2 = transactions[i]
        delta = to_unix_timestamp(t2['timestamp']) - to_unix_timestamp(t1['timestamp'])
        if t1['destination_country'] != t2['destination_country'] and delta <= 60:
            suspicious.append(t2 | {'fraud_reason': 'Suspicious country switch'})
    return suspicious

def flag_multiple_cards_same_phone(transactions):
    transactions = sorted(transactions, key=lambda x: to_unix_timestamp(x['timestamp']))
    seen_cards = {}
    suspicious = []
    for txn in transactions:
        ts = to_unix_timestamp(txn['timestamp'])
        phone = txn['phone_number']
        card = txn['card_number']
        if phone not in seen_cards:
            seen_cards[phone] = []
        seen_cards[phone] = [(c, t) for c, t in seen_cards[phone] if ts - t <= 60]
        if any(c != card for c, _ in seen_cards[phone]):
            suspicious.append(txn | {'fraud_reason': 'Multiple cards on same phone'})
        seen_cards[phone].append((card, ts))
    return suspicious

def flag_large_amounts(transactions):
    return [txn | {'fraud_reason': 'Large transaction amount'} for txn in transactions if txn['amount'] > 1000]

def flag_rapid_transactions(transactions):
    transactions = sorted(transactions, key=lambda x: to_unix_timestamp(x['timestamp']))
    suspicious = []
    for i in range(len(transactions)):
        count = 1
        base_time = to_unix_timestamp(transactions[i]['timestamp'])
        for j in range(i+1, len(transactions)):
            if to_unix_timestamp(transactions[j]['timestamp']) - base_time <= 60:
                count += 1
            else:
                break
        if count >= 3:
            suspicious.extend([txn | {'fraud_reason': 'Rapid transactions'} for txn in transactions[i:i+count]])
    return suspicious

# --- Main function that applies the rules ---
def detect_fraud(transactions):
    suspicious = []
    suspicious += flag_suspicious_country_switch(transactions)
    suspicious += flag_multiple_cards_same_phone(transactions)
    suspicious += flag_large_amounts(transactions)
    suspicious += flag_rapid_transactions(transactions)
    return suspicious

# --- Pipeline ---
def run_pipeline():
    options = PipelineOptions(
    #runner='DirectRunner')
    runner='DataflowRunner',  
    project='felix-pago-457321',  
    temp_location='gs://bucket-felix-pago/temp/', 
    region='us-central1',
    save_main_session=True,
    job_name='fraud-detection-job' ) 

    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        lines = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/felix-pago-457321/subscriptions/transactions-sub')
            | 'Decode bytes' >> beam.Map(lambda x: x.decode('utf-8'))
        )

        raw_transactions = (
            lines
            | 'Parse JSON' >> beam.Map(parse_json)
        )

        suspicious_transactions = (
            raw_transactions
            | 'Window into Fixed Intervals' >> beam.WindowInto(beam.window.FixedWindows(60))
            | 'Group by user_id' >> beam.GroupBy(lambda x: x['user_id'])
            | 'Detect Fraud' >> beam.FlatMap(lambda kv: detect_fraud(kv[1]))
        )

        # Suspicious transactions to BigQuery
        suspicious_transactions | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        table='felix-pago-457321:fraud_detection.suspicious_transactions',
        schema='transaction_id:STRING, user_id:STRING, amount:FLOAT, origin_country:STRING, destination_country:STRING, payment_method:STRING, timestamp:STRING, card_number:STRING, phone_number:STRING, fraud_reason:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location='gs://bucket-felix-pago/temp'
        )

# Local execution
if __name__ == '__main__':
    run_pipeline()
