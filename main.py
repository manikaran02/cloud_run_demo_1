from flask import Flask, request, jsonify
import base64

app = Flask(__name__)

@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    try:
        envelope = request.get_json()
        if not envelope:
            msg = 'no Pub/Sub message received'
            print(f'error: {msg}')
            return f'Bad Request: {msg}', 400

        pubsub_message = envelope['message']

        if 'data' in pubsub_message:
            message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            print(f'Received message: {message_data}')

        attributes = pubsub_message.get('attributes', {})
        for key, value in attributes.items():
            print(f'Attribute {key}: {value}')

        return jsonify(status='success'), 200

    except Exception as e:
        print(f'Error: {e}')
        return jsonify(status='error', message=str(e)), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
