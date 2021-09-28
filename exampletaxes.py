import os

import dotenv

import cryptotax

# load environment variables containing my Coinbase API keys
dotenv.load_dotenv()

# Set Coinbase API keys from environment
key = os.getenv("coinbase_key")
b64secret = os.getenv("coinbase_b64secret")
passphrase = os.getenv("coinbase_passphrase")

# Location of the modified "fills" document downloaded from Coinbase pro.
# Coinbase pro provides users with a complete record of their trading activity.
# A "fill" means an order submitted to Coinbase was executed, or "filled". See README.md for more.
# So this is a list of all filled orders.
fillspath = "example_fills_data/fills_2019.csv"

# Create the CryptoTax object and give it the Coinbase API keys
ct = cryptotax.CryptoTax(key, b64secret, passphrase)

# Create the pricelogs used to look up historical prices (see README.md for more)
pricelogs = ct.readFillsForPrices(fillspath)

# Loop through the fills and add to or pull from holdings, generating transactions whenever a crypto asset is disposed of.
ct.readFillsForGains(fillspath, pricelogs)

# Write the transactions to a csv in a format easily transferable to IRS form 8949
ct.writeTransactions("transactions.csv")
