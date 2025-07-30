from flask import Flask

app = Flask(__name__)

@app.route('/')
def helloWorld():
    return "Hey! I just used a flask app..."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)