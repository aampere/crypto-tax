import bisect
import csv
import time
import datetime
import coinutil as cu
import cbpro

#replace with Coinbase pro API keys and passphrase
key = ''
b64secret = ''
passphrase = ''

#Create cbpro objects to make API calls to coinbase
public_client = cbpro.PublicClient()
auth_client = cbpro.AuthenticatedClient(key, b64secret, passphrase)

#coinutil is a library I wrote that contains common utilities for my Coinbase API related projects.
#MarketInfo mostly just keeps track of the current status of currency markets on Coinbase (eg, active, limited, disabled) 
mi = cu.MarketInfo(public_client,auth_client)

#location of the modified "fills" document downloaded from Coinbase pro.
#Coinbase pro provides users with a complete record of their trading activity.
#A "fill" means an order submitted to Coinbase was executed, or "filled". See word document for more.
#So this is a list of all filled orders.
fillspath = 'cryptotax/fills_2020_github.csv'

#A "holding" is a quantity of a cryptocurrency, its value in USD at time of acquisition ("basis") and date/time acquired.
#holdings is a dict that maps currency names to a list of holdings.
holdings = {}

#Transactions are what will ultimately populate IRS form 8949.
#When we "dispose" of a crypto asset, ie, convert it to USD or another cryptocurrency in a market,
#we will construct a "transaction" which records the date/time the asset was acquired,
#and the gain/loss from the transaction.
#Each transaction is a {description, date acquired, date disposed, proceeds, cost or basis, gain or loss}
transactions = []

#This program will use a FIFO ("First In, First Out") model to determine the
#gains or losses from a disposal of crypto assets. This is the typical way capital
#gains and losses are calculated by a stock broker for clients' tax documents.
#We will run through the fills and add to or pull from holdings, pulling from the OLDEST holding available (FIFO)
#Transactions are created each time a fill is a disposal of crypto, and the transaction list is what the IRS wants ultimately.

#Creates an incomplete history of prices for each currency we've traded in the past.
#This is needed because for crypto-to-crypto trades (eg, trading Bitcoin for Ethereum),
#that is still considered a disposal of assets, and capital gain/loss must be assessed
#based on the "current fair market value" of the procured currency. We could query the API for that
#historical price point, which is request-limited, or we could look for a price from our fills doc from a close-by time.
def readFillsForPrices(fp):
    pricelogs = {} # dict with basecurrency pointing to ordered pairs of (timestamp,price) where price is price in USD
    #the csv is already ordered by time
    with open(fp, newline='') as fillscsv:
        fillsreader = csv.DictReader(fillscsv, delimiter=',')
        for row in fillsreader:
            if row['price/fee/total unit'] != 'USD':
                continue
            curr = row['size unit']
            if not curr in pricelogs:
                pricelogs[curr] = []
            pricelogs[curr].append((float(row['timestamp']),float(row['price'])))
    return pricelogs

#The primary work of this program.
#Loop through the fills and add to or pull from holdings;
#Generate transactions whenever a crypto asset is disposed of.
#The 'BUY' and 'SELL' side terminology is Coinbase's, and extremely important to keep straight.
def readFillsForGains(mi,fp,pricelogs):
    with open(fillspath, newline='') as fillscsv:
        fillsreader = csv.DictReader(fillscsv, delimiter=',')
        i=0
        for row in fillsreader:
            print('--------------')
            print('i: {0}'.format(i))
            quotecurrency = row['product'].split('-')[1]
            basecurrency = row['size unit']
            side = row['side']
            print('{0} in {1}-{2}   tradeid:{3}'.format(side, basecurrency, quotecurrency, row['trade id']))
            if quotecurrency=='USD' and side=='BUY':
                #We are buying crypto with USD
                addToHoldings(basecurrency, float(row['size']), -float(row['total']),row['yyyy'],row['mm'],row['dd']) #Total is how much USD we spent, including fee, to procure size
                #There is no gain/loss to recognize.
            elif quotecurrency!='USD' and side=='BUY':
                #We are buying crypto with BTC or ETH. These are the only cryptos used to buy other cryptos. In the docs below and variable names I write like this is BTC, even though it could be ETH.
                
                #We are exchanging BTC for property (another crypto)
                #This is recognizing a gain or loss between the basis of BTC (in USD) and the value of the procured crypto (in USD)
                #Pull out the total BTC we used to place the buy. A list of BTC holdings, each with possibly different bases and times is created, the sum of whose sizes is the total BTC
                holdinglist = pullFromHoldings(quotecurrency,-float(row['total'])) #total is how much BTC we spent, including fee, to procure size of other currency
                
                basecurrencyprice = closestPrice(mi,basecurrency,pricelogs[basecurrency],float(row['timestamp']))
                usdvalueofcrypto = basecurrencyprice*float(row['size'])
                
                #Recognize loss/gain of size usdvalueofcrypto-(total basis of btc)
                totalbasisbtc = 0.0 #This will be "cost or other basis" in IRS form 8949
                dateacquired = ''
                for holding in holdinglist:
                    totalbasisbtc += holding['usdbasis']
                    subdateacquired = '{0}/{1}/{2}'.format(holding['mm'],holding['dd'],holding['yyyy'])
                    if dateacquired == '':
                        dateacquired = subdateacquired
                    if subdateacquired != dateacquired:
                        dateacquired = 'VARIOUS' #If the assets being disposed of were acquired over multiple dates, the "date acquired" entry in the form will read "VARIOUS"
                datesold = '{0}/{1}/{2}'.format(row['mm'],row['dd'],row['yyyy'])
                desc = '{0} {1} (virtual currency)'.format(row['total'][1:len(row['total'])], quotecurrency) #remove the minus sign from the total. This is a human-readable string that will go in the IRS form, the number should just be shown unsigned
                transactions.append({'description':desc,'dateacquired':dateacquired,'datesold':datesold,'proceeds':usdvalueofcrypto,'cost':totalbasisbtc,'gain':usdvalueofcrypto-totalbasisbtc,'row':row,'holdinglist':holdinglist})
                
                #Finally, add the new currency to our holdings
                addToHoldings(basecurrency, float(row['size']), usdvalueofcrypto, row['yyyy'],row['mm'],row['dd'])
                
            elif quotecurrency!='USD' and side=='SELL':
                #We are selling crypto for BTC or ETH. below i write as if the quote currency is BTC, even though it could also be ETH
                
                #We are exchanging a crypto for BTC
                #This is recognizing a gain or loss between the basis of the crypto (in USD) and the value of the procured BTC (in USD)
                #Pull out the total crypto used to place the sell. A list of crypto holdings, each with possibly different bases and times is created, the sum of whose sizes is the total crypto
                holdinglist = pullFromHoldings(basecurrency,float(row['size'])) #size is how much crypto we sold.
                
                quotecurrencyprice = closestPrice(mi,quotecurrency,pricelogs[quotecurrency],float(row['timestamp']))
                usdvalueofquotecurrency = quotecurrencyprice*float(row['total']) #total is the amount of BTC we procured, less fee
                
                #Recognize loss/gain of size usdvalueofquotecurrency-(total basis of crypto)
                #Sum up the bases in holdinglist, recognize the full gain or loss between usdvalueofquotecurrency and that full basis, and use "VARIOUS" as the date acquired.
                totalbasiscrypto = 0.0
                dateacquired = ''
                for holding in holdinglist:
                    totalbasiscrypto += holding['usdbasis']
                    subdateacquired = '{0}/{1}/{2}'.format(holding['mm'],holding['dd'],holding['yyyy'])
                    if dateacquired == '':
                        dateacquired = subdateacquired
                    if subdateacquired != dateacquired:
                        dateacquired = 'VARIOUS'  #If the assets being disposed of were acquired over multiple dates, the "date acquired" entry in the form will read "VARIOUS"
                datesold = '{0}/{1}/{2}'.format(row['mm'],row['dd'],row['yyyy'])
                desc = '{0} {1} (virtual currency)'.format(row['size'], basecurrency)
                transactions.append({'description':desc,'dateacquired':dateacquired,'datesold':datesold,'proceeds':usdvalueofquotecurrency,'cost':totalbasiscrypto,'gain':usdvalueofquotecurrency-totalbasiscrypto,'row':row,'holdinglist':holdinglist})
                
                #Finally, add the new BTC to our holdings
                addToHoldings(quotecurrency, float(row['total']), usdvalueofquotecurrency, row['yyyy'],row['mm'],row['dd'])
                
            elif quotecurrency=='USD' and side=='SELL':
                #We are selling crypto for USD.
                
                #This is recognizing a loss between the basis of the crypto (in USD) and the USD procured
                #Pull out the total crypto we used to place the sell. a list of crypto holdings, each with possible different bases and times is created, the some of whose sizes it the total crypto
                holdinglist = pullFromHoldings(basecurrency,float(row['size'])) #
                
                #Recognize the loss/gain of size totalusd-totalbasiscrypto 
                totalusd = float(row['total'])
                totalbasiscrypto = 0.0
                dateacquired = ''
                for holding in holdinglist:
                    totalbasiscrypto += holding['usdbasis']
                    subdateacquired = '{0}/{1}/{2}'.format(holding['mm'],holding['dd'],holding['yyyy'])
                    if dateacquired == '':
                        dateacquired = subdateacquired
                    if subdateacquired != dateacquired:
                        dateacquired = 'VARIOUS'  #If the assets being disposed of were acquired over multiple dates, the "date acquired" entry in the form will read "VARIOUS"
                datesold = '{0}/{1}/{2}'.format(row['mm'],row['dd'],row['yyyy'])
                desc = '{0} {1} (virtual currency)'.format(row['size'], basecurrency)
                transactions.append({'description':desc,'dateacquired':dateacquired,'datesold':datesold,'proceeds':totalusd,'cost':totalbasiscrypto,'gain':totalusd-totalbasiscrypto,'row':row,'holdinglist':holdinglist})
                
            print('--------------')
            i=i+1

def addToHoldings(curr, size, usdbasis, yyyy, mm, dd):
    #curr: currency being held
    #size: size (amount) of currency being held
    #usdbasis: how much USD it took to get this size of currency
    if not curr in holdings:
        holdings[curr] = [] #if there is no list of holdings yet for this currency, create it.
    holdings[curr].append({'size':size, 'usdbasis':usdbasis, 'yyyy':yyyy, 'mm':mm, 'dd':dd})
    
def pullFromHoldings(curr, amt):
    #This returns a list of holdings for the curr.
    #each holding has {'size':size, 'usdbasis':usdbasis, 'yyyy':yyyy, 'mm':mm, 'dd':dd}
    #amt is coming in as a float
    global holdings
    pulledholdinglist = [] #this will be returned. its a list of holdings whose size add up to the amt
    holding = holdings[curr] #list of holdings in the currency. earliest acquired is index 0.
    print('curr: {0}  amt: {1}'.format(curr,amt))
    amtleft = amt
    while True:
        print('*{0}'.format(amtleft))
        print('*{0}'.format(holding))
        if amtleft < holding[0]['size']:
            #the oldest holding is bigger than the amount we are pulling.
            #decrement the oldest holding by amtleft
            basis = amtleft/holding[0]['size']*holding[0]['usdbasis']
            holding[0]['size'] = holding[0]['size'] - amtleft
            holding[0]['usdbasis'] = holding[0]['usdbasis'] - basis
            pulledholdinglist.append({'size':amtleft, 'usdbasis':basis, 'yyyy':holding[0]['yyyy'], 'mm':holding[0]['mm'], 'dd':holding[0]['dd']})
            break
        else:
            #the oldest holding is equal or smaller than the amount we are pulling
            amtleft -= holding[0]['size']
            pulledholdinglist.append(holding.pop(0))
            if len(holding) == 0:
                print('there were no holdings left with amtleft:{0}'.format(amtleft)) #This "warning" only matters if it says the amtleft is >0.0. (ie, that's a problem beause we want to pull more from holdings, but there's no holdings left.) It SHOULD print this warning with 0.0 if you just drew down the last of your holdings exactly.
                break
            if amtleft <= 0:
                print('while pulling amtleft was <= 0  amtleft:{0}'.format(amtleft)) #This "warning" only matters if amtleft is truly negative. That can't happen due to the enclosing if statement. That means this "warning" SHOULD display with 0.0 when the holding drawn from was EXACTLY the size of amtleft.
                break
    return pulledholdinglist
    
#Binary search of the historical price data generated from our fills doc.
#If an entry is not found within 30 seconds of the queried time, get the historic price form the API
def closestPrice(mi, curr, pricelog, timestamp):
    #pricelog is list of ordered pairs of (timestamp,price) where price is in USD
    if len(pricelog)==0:
        print("NO PRICE AVAILABLE")
        return getHistoricPrice(mi, curr, timestamp)
    index = bisect.bisect(pricelog, (timestamp,0))
    if index == 0:
        entry = pricelog[0]
    elif index == len(pricelog):
        entry = pricelog[-1]
    else:
        if abs(pricelog[index][0]-timestamp) < abs(pricelog[index-1][0]-timestamp):
            entry = pricelog[index]
        else:
            entry = pricelog[index-1]
    if abs(entry[0]-timestamp) < 30.0:
        return entry[1]
    else:
        print("NO PRICE ENTRY CLOSE ENOUGH")
        time.sleep(1.01) #Avoid too many requests.
        return getHistoricPrice(mi, curr, timestamp)

#Obtains historical price for a currency. Used only in case our fills document did not already provide a close-enough price.
#Searches first for exact candle (within 1 minute), then requests larger candles if smaller candles are unavailable.
#(A "candle" is the opening, closing, high, and low prices for a currency within a time period)
def getHistoricPrice(mi, curr, timestamp):
    #Statement from CB Pro provides ISO 8601 string for date/time
    #In excel, I convert that to POSIX timestamp, ie seconds since jan 1 1970. 
    #utcfromtimestamp() creates a datetime object from timestamp.
    #isoformat() converts it back to the ISO 8601 format.
    #When I take the statement ISO timestamp, use excel to calculate epocseconds, then use isoformat on that number, i get the original ISO string back again, meaning everything is on the correct standard.
    #Coinbase's auth_client_get_product_historic_rates takes ISO strings as start/end parameters, but returns time in timestamp. Eyeroll.
    #The returned candle is  [bucketstarttime, low, high, open, close, volume]
    
    print('Attempting to find historical USD price for {0} at {1}'.format(curr, timestamp))
    
    #this attempts to capture the exact candle of size 60s that contains the time requested
    info = mi.auth_client.get_product_historic_rates(curr+'-USD',start=datetime.datetime.utcfromtimestamp(timestamp-60).isoformat(),end=datetime.datetime.utcfromtimestamp(timestamp).isoformat(), granularity=60)
    if type(info)==list:
        if len(info)==1:
            candle = info[0]
            return (candle[3]+candle[4])*0.5
        elif len(info)==0:
            print('the list thats supposed to have historical data in it is empty')
        else:
            print('We tried to get 1 candle, but we got more back, using the first candle')
            candle = info[0]
            return (candle[3]+candle[4])*0.5
    else:
        print('oops, we got something non-list back instead of a list of historical data.')
    
    time.sleep(1.0)
    
    #Expand the queried times, find the best time
    print('trying expanded second query of historical data')
    info = mi.auth_client.get_product_historic_rates(curr+'-USD',start=datetime.datetime.utcfromtimestamp(timestamp-1200).isoformat(),end=datetime.datetime.utcfromtimestamp(timestamp+1200).isoformat(), granularity=60)
    if type(info)==list:
        if len(info)==0:
            print('the list thats supposed to have historical data in it is empty')
        else:
            closestprice = 0.0
            deltat = 100000000
            for candle in info:
                if abs(candle[0]-timestamp)<deltat:
                    deltat = abs(candle[0]-timestamp)
                    closestprice = (candle[3]+candle[4])*0.5
            return closestprice
    else:
        print('oops, we got something non-list back instead of a list of historical data.')
    
    time.sleep(1.0)
    
    #Expand the queried times, granularity 1hr, find the best time. search +/- 12 hrs.
    print('Trying expanded hour query of historical data')
    info = mi.auth_client.get_product_historic_rates(curr+'-USD',start=datetime.datetime.utcfromtimestamp(timestamp-45000).isoformat(),end=datetime.datetime.utcfromtimestamp(timestamp+45000).isoformat(), granularity=3600)
    if type(info)==list:
        if len(info)==0:
            print('the list thats supposed to have historical data in it is empty')
        else:
            closestprice = 0.0
            deltat = 100000000
            for candle in info:
                if abs(candle[0]-timestamp)<deltat:
                    deltat = abs(candle[0]-timestamp)
                    closestprice = (candle[3]+candle[4])*0.5
            return closestprice
    else:
        print('oops, we got something non-list back instead of a list of historical data.')
    
    #We can't find historical price data, something is seriously wrong somewhere, or API is down/disfunctional.
    print('Could not find good historical price')
    raise Exception('Could not find good historical price')
    
def sumHoldings(holding):
    totalsize = 0.0
    for h in holding:
        totalsize += h['size']
    return totalsize

#Write transactions to csv in a way that can easily be transferred to IRS form 8949
def writeTransactions(trans, path):
    with open(path, 'w', newline='') as f:
        fieldnames = ['description', 'dateacquired', 'datesold', 'proceeds', 'cost', 'gain']
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    
        writer.writeheader()
        for t in trans:
            writer.writerow(t)
    
    
pricelogs = readFillsForPrices(fillspath)
readFillsForGains(mi,fillspath,pricelogs)