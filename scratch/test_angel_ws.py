import time
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import tara_config as config

def on_data(ws, message):
    print("Ticks: {}".format(message))

def on_open(ws):
    print("on_open triggered - subscribing to SBIN...")
    token_list = [{"exchangeType": 1, "tokens": ["3045"]}] # SBIN
    ws.subscribe("test_id", 1, token_list)
    print("Subscribe called")

def on_error(ws, error):
    print("on_error: {}".format(error))

def on_close(ws):
    print("on_close")

def test_connection():
    obj = SmartConnect(api_key=config.API_KEY)
    data = obj.generateSession(config.CLIENT_ID, config.PWD, pyotp.TOTP(config.TOTP_KEY).now())
    
    if not data.get("status"):
        print("Login failed: {}".format(data))
        return

    print("Data keys: {}".format(data.get("data", {}).keys()))
    feed_token = obj.getfeedToken()
    jwt_token = data["data"]["jwtToken"]

    if jwt_token.startswith("Bearer "):
        jwt_token = jwt_token.replace("Bearer ", "")
    
    print("Login successful. JWT (clean): {}... Feed: {}".format(jwt_token[:10], feed_token))
    
    ws = SmartWebSocketV2(jwt_token, config.API_KEY, config.CLIENT_ID, feed_token)
    ws.on_open = on_open
    ws.on_data = on_data
    ws.on_error = on_error
    ws.on_close = on_close
    
    ws.connect()

if __name__ == "__main__":
    test_connection()
