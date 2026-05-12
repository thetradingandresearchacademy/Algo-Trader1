from SmartApi import SmartConnect
import pyotp


class AngelAuth:

    def __init__(self, api_key, client_code, password, totp_key):

        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.totp_key = totp_key

        self.obj = SmartConnect(api_key=self.api_key)

    def login(self):

        totp = pyotp.TOTP(self.totp_key).now()

        data = self.obj.generateSession(
            self.client_code,
            self.password,
            totp
        )

        jwt_token = data['data']['jwtToken']
        refresh_token = data['data']['refreshToken']

        feed_token = self.obj.getfeedToken()

        return jwt_token, refresh_token, feed_token