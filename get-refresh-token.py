import ConfigParser
import globus_sdk

Config = ConfigParser.ConfigParser()
Config.read("config.ini")
client_id = Config.get("auth", "client_id")

client = globus_sdk.NativeAppAuthClient(client_id)
client.oauth2_start_flow(refresh_tokens=True)

print('Please go to this URL and login: {0}'
              .format(client.oauth2_get_authorize_url()))

get_input = getattr(__builtins__, 'raw_input', input)
auth_code = get_input('Please enter the code here: ').strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

# let's get stuff for the Globus Transfer service
globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']
# the refresh token and access token, often abbr. as RT and AT
transfer_rt = globus_transfer_data['refresh_token']
transfer_at = globus_transfer_data['access_token']
expires_at_s = globus_transfer_data['expires_at_seconds']

print("Refresh Token: '" + str(transfer_rt) + "'")
print("Active Token: '" + str(transfer_at) + "'")
print("Expires '" + str(expires_at_s) + "'")
