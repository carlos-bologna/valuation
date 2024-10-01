from flask import Flask, jsonify
import bronze
import silver
import gold_stock_price_labeled

app = Flask(__name__)

@app.route('/transformation', methods=['GET'])
def run_pipeline():
    try:
        # Run bronze layer processing
        bronze.main()

        # Run silver layer processing
        silver.main()

        # Run gold layer processing
        gold_stock_price_labeled.main()

        return jsonify({"message": "Pipeline executed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
