# MarketInfo contains the Coinbase pro clients, as well as a list of available products and possible triangles.
# A triangle is my term for a three-part transaction that converts USD to a crypto, to another crypto, then back to USD.
# One motivation for this project was to investigate split-second arbitrage opportunites in these triangles.
class MarketInfo:
    def __init__(self, p_c, a_c):  # pass in intiated public and authenticated clients
        self.public_client = p_c
        self.auth_client = a_c
        self.products = self.public_client.get_products()
        self.productsdict = {}
        self.currencies = []
        self.productids = []
        self.biproductids = []
        self.alltriangles = []
        self.usdtriangles = []
        self.usdtrianglesproductids = []
        for p in self.products:
            blacklist = ["GBP", "EUR"]
            badproduct = False
            # don't add the product if I can't place market orders.
            if (
                p["post_only"] == True
                or p["limit_only"] == True
                or p["cancel_only"] == True
                or p["trading_disabled"] == True
                or p["status"] != "online"
            ):
                print("A market is partially disabled")
                print(p)
                badproduct = True
            for bl in blacklist:
                if bl in p["id"]:
                    badproduct = True
            if badproduct:
                continue
            pid = p["id"]
            self.productids.append(pid)
            self.biproductids.append(pid)
            self.biproductids.append(reverseID(pid))
            self.productsdict[pid] = p
            parts = pid.split("-")
            if parts[0] not in self.currencies:
                self.currencies.append(parts[0])
            if parts[1] not in self.currencies:
                self.currencies.append(parts[1])
        i = 0
        while i != len(self.biproductids):
            j = i + 1
            while j != len(self.biproductids):
                if (
                    self.biproductids[i].split("-")[1]
                    == self.biproductids[j].split("-")[0]
                    and (
                        self.biproductids[j].split("-")[1]
                        + "-"
                        + self.biproductids[i].split("-")[0]
                    )
                    in self.biproductids
                ):
                    pa0 = ProductAction(self.biproductids[i], self.productids)
                    pa1 = ProductAction(self.biproductids[j], self.productids)
                    pa2 = ProductAction(
                        self.biproductids[j].split("-")[1]
                        + "-"
                        + self.biproductids[i].split("-")[0],
                        self.productids,
                    )
                    tri = Triangle(pa0, pa1, pa2)
                    if (not tri.reorderSyn(1) in self.alltriangles) and (
                        not tri.reorderSyn(2) in self.alltriangles
                    ):
                        self.alltriangles.append(tri)
                j += 1
            i += 1
        for tri in self.alltriangles:
            if tri.hasCurrency("USD"):
                triusd = tri.reorderSyn(0)
                triusd.reorderToBeginWith("USD")
                self.usdtriangles.append(triusd)
        for tri in self.usdtriangles:
            for i in range(3):
                if not tri.tri[i].trueid in self.usdtrianglesproductids:
                    self.usdtrianglesproductids.append(tri.tri[i].trueid)

    def updateProducts(self, statusmsg):
        sprods = statusmsg.get("products", {})
        for p in self.products:
            for sp in sprods:
                if p["id"] == sp["id"]:
                    p = sp  # replace the mi's product info with the new product info coming in from the websocket. Note: although it seems to me that the dict coming in through the status channel should have identical keys to the product info obtained by auth_client.get_products, they are slightly different. Notably, the product in the status channel is missing the 'trading_disabled' key. I emailed help about this, it doesn't seem right. There are other differences, but this is the main one.
                    self.productsdict[p["id"]] = sp

    def productOK(self, pid):
        p = self.productsdict[pid]
        ok = (
            p.get("post_only", True) == False
            and p.get("limit_only", True) == False
            and p.get("cancel_only", True) == False
            and p.get("status", "") == "online"
        )
        return ok

    def triangleOK(self, triangle):
        return (
            self.productOK(triangle.pa0.trueid)
            and self.productOK(triangle.pa1.trueid)
            and self.productOK(triangle.pa2.trueid)
        )


class ProductAction:
    '''
    ProductAction 'X-Y' means "convert Y to X" (This convention is intentional, as it makes chained product actions readable from back-to-front)
    nameid is the supplied string. indiates currency types and "direction"
    trueid is the true id present on the exchange (eg, "USD-BTC" is a valid nameid which indicates converting BTC to USD, and "BTC-USD" is the trueid name of the market on Coinbase on which either buys or sells can happen)
    action is the action required on the trueid to get the direction supplied by the nameid
    '''
    def __init__(self, ids, fulltrueidlist):
        self.nameid = ids
        self.trueidlist = fulltrueidlist
        if self.nameid in self.trueidlist:
            self.trueid = self.nameid
            self.action = "buy"
        elif reverseID(self.nameid) in self.trueidlist:
            self.trueid = reverseID(self.nameid)
            self.action = "sell"
        else:
            print(
                "Supplied nameid %s or its reverse could not be found in the product list"
                % self.nameid
            )

    def __eq__(self, other):
        if isinstance(other, ProductAction):
            return self.nameid == other.nameid
        return False

    def left(self):
        return self.nameid.split("-")[0]

    def right(self):
        return self.nameid.split("-")[1]

    def hasCurrency(self, cur):
        parts = self.nameid.split("-")
        return parts[0] == cur or parts[1] == cur


class Triangle:
    '''
    A triangle is my term for a three-part transaction that converts USD to a crypto, to another crypto, then back to USD.
    One motivation for this project was to investigate split-second arbitrage opportunites in these triangles.
    The Triangle class is essentially a list of three products, validated to be a triangle, and helper functions to get info about or reorder the triangle
    '''
    def __init__(self, pa_0, pa_1, pa_2):
        self.pa0 = pa_0
        self.pa1 = pa_1
        self.pa2 = pa_2
        self.tri = [self.pa0, self.pa1, self.pa2]

    def __eq__(
        self, other
    ):  # This checks for identical order too. equal cycles with different starting currencies are inequal according to this
        if isinstance(other, Triangle):
            return (
                self.tri[0] == other.tri[0]
                and self.tri[1] == other.tri[1]
                and self.tri[2] == other.tri[2]
            )
        return False

    def hasCurrency(self, cur):
        for pa in self.tri:
            if pa.hasCurrency(cur):
                return True
        return False

    def hasProduct(self, pid):
        # is the product present in natural or reverse direction?
        for pa in self.tri:
            if pa.trueid == pid or pa.trueid == reverseID(pid):
                return True
        return False

    def reorderToBeginWith(self, cur):
        # reorder if any pa has the cur. if the order is already good, or the cur is not contained in any pa, do nothing.
        triorig = self.tri.copy()
        if triorig[0].right() == cur:
            self.tri[0] = triorig[1]
            self.tri[1] = triorig[2]
            self.tri[2] = triorig[0]
        elif triorig[1].right() == cur:
            self.tri[0] = triorig[2]
            self.tri[1] = triorig[0]
            self.tri[2] = triorig[1]

    def reorder(self, steps):
        triorig = self.tri.copy()
        self.tri[0] = triorig[(0 + steps) % 3]
        self.tri[1] = triorig[(1 + steps) % 3]
        self.tri[2] = triorig[(2 + steps) % 3]

    def reorderSyn(self, steps):
        trisyn = Triangle(self.tri[0], self.tri[1], self.tri[2])
        trisyn.reorder(steps)
        return trisyn

    def idChain(self):
        return self.tri[0].nameid + " " + self.tri[1].nameid + " " + self.tri[2].nameid

    def shortIdChain(self):
        return (
            self.tri[0].nameid.split("-")[0]
            + "-"
            + self.tri[1].nameid
            + "-"
            + self.tri[2].nameid.split("-")[1]
        )

    def printIDs(self):
        print(self.tri[0].nameid + " " + self.tri[1].nameid + " " + self.tri[2].nameid)


def reverseID(pid):
    return pid.split("-")[1] + "-" + pid.split("-")[0]


def trianglesEqual(tri1, tri2):
    return (
        (tri1[0] == tri2[0] and tri1[1] == tri2[1] and tri1[2] == tri2[2])
        or (tri1[0] == tri2[1] and tri1[1] == tri2[2] and tri1[2] == tri2[0])
        or (tri1[0] == tri2[2] and tri1[1] == tri2[0] and tri1[2] == tri2[1])
    )


def triangleSyn1(tri):
    return (tri[1], tri[2], tri[0])


def triangleSyn2(tri):
    return (tri[2], tri[0], tri[1])


def reducePrecision(amount_str, increment_str):
    #Format a string containing a number to the correct precision base on the increment provided by API
    parts_inc = increment_str.split(".")
    parts_amt = amount_str.split(".")
    if len(parts_inc) < 2:
        parts_inc.append("0")
    if len(parts_amt) < 2:
        parts_amt.append("0")
    amt_prec = ""
    if "1" in parts_inc[0]:
        # inc is 1.0 or greater
        place = len(parts_inc[0]) - parts_inc[0].find("1")
        for i in range(len(parts_amt[0])):
            if i > len(parts_amt[0]) - place:
                amt_prec += "0"
            else:
                amt_prec += parts_amt[0][i]
        amt_prec += ".0"
    elif "1" in parts_inc[1]:
        # inc is 0.1 or less
        amt_prec += parts_amt[0] + "."
        place = parts_inc[1].find("1")
        for i in range(len(parts_amt[1])):
            if i > place:
                amt_prec += "0"
            else:
                amt_prec += parts_amt[1][i]
    return amt_prec

'''
When you make a "market buy" you will accept the best asks in order on the order book until your order size is filled.
As a result, the price may vary as your order is filled, and you don't know the size of your fill a priori.
For large orders, particularly on smaller markets, the price could change a lot ("slippage")
These functions run through the order book and estimate final filled size.
'''
def estimateMarketBuyFilledSize(mi, pid, funds, level2book=0):
    l2b = (
        level2book
        if level2book
        else mi.public_client.get_product_order_book(pid, level=2)
    )
    fundsremaining = funds
    filledsize = 0.0
    for ask in l2b["asks"]:
        price = float(ask[0])
        volume = float(ask[1])
        valueofask = price * volume
        if fundsremaining <= valueofask:
            filledsize += fundsremaining / price
            fundsremaining = 0.0
            break
        else:
            filledsize += volume
            fundsremaining -= valueofask
    return filledsize


def estimateMarketSellExecutedValue(mi, pid, size, level2book=0):
    l2b = (
        level2book
        if level2book
        else mi.public_client.get_product_order_book(pid, level=2)
    )
    sizeremaining = size
    executedvalue = 0.0
    for bid in l2b["bids"]:
        price = float(bid[0])
        volume = float(bid[1])
        if sizeremaining <= volume:
            executedvalue += price * sizeremaining
            sizeremaining = 0.0
            break
        else:
            executedvalue += volume * price
            sizeremaining -= volume
    return executedvalue

'''
Coinbase fees are less if you are on the "maker" side of an order, ie you place a limit order and wait for someone else to fill your order.
These functions look at the order book and calculates the limit order that will place your order at the front of the order book.
Simply put, this is an attempt to buy/sell at market price immediately, but with a limit order to reduce fees.
There is always the chance that your order will be undercut/overcut as the market moves though, and not filled immediately.
'''
def getBestLimitBuyInfo(mi, pid, funds, level2book=0):
    l2b = (
        level2book
        if level2book
        else mi.public_client.get_product_order_book(pid, level=2)
    )
    bestbid = l2b["bids"][0]
    bestbidprice = float(bestbid[0])
    bestask = l2b["asks"][0]
    bestaskprice = float(bestask[0])
    limitbidprice = bestbidprice  # The price I will ultimately bid
    quoteincrement = float(mi.productsdict[pid]["quote_increment"])
    if (
        bestaskprice - bestbidprice > 5.0 * quoteincrement
    ):  # If the spread is greater than eg 0.05 USD, bid two cents higher
        limitbidprice = bestaskprice - 2.0 * quoteincrement
    limitbidprice_str = reducePrecision(
        str(limitbidprice), mi.productsdict[pid]["quote_increment"]
    )
    limitbidprice = float(limitbidprice_str)
    limitbidvolume = funds / limitbidprice
    limitbidvolume_str = reducePrecision(
        str(limitbidvolume), mi.productsdict[pid]["base_increment"]
    )
    return (limitbidprice_str, limitbidvolume_str)


def bestLimitBuy(mi, pid, funds, level2book=0, coid=""):
    l2b = (
        level2book
        if level2book
        else mi.public_client.get_product_order_book(pid, level=2)
    )
    pricesize = getBestLimitBuyInfo(mi, pid, funds, l2b)
    limitbidprice_str = pricesize[0]
    limitbidvolume_str = pricesize[1]
    return mi.auth_client.buy(
        product_id=pid,
        order_type="limit",
        size=limitbidvolume_str,
        price=limitbidprice_str,
        client_oid=coid,
    )


def getBestLimitSellInfo(mi, pid, size, level2book=0):
    l2b = level2book if level2book else mi.public_client.get_product_order_book(pid, level=2)
    bestbid = l2b["bids"][0]
    bestbidprice = float(bestbid[0])
    bestask = l2b["asks"][0]
    bestaskprice = float(bestask[0])
    limitaskprice = bestaskprice # the price I will ultimately ask
    quoteincrement = float(mi.productsdict[pid]['quote_increment'])
    if  bestaskprice-bestbidprice>5.0*quoteincrement: #If the spread is greater than eg 0.05 USD, ask two cents lower
        limitaskprice = bestbidprice+2.0*quoteincrement
    limitaskprice_str = reducePrecision(str(limitaskprice), mi.productsdict[pid]['quote_increment'])
    limitaskprice = float(limitaskprice_str)
    limitaskvolume_str = reducePrecision(
        str(size), mi.productsdict[pid]["base_increment"]
    )
    return (limitaskprice_str, limitaskvolume_str)
def bestLimitSell(mi, pid, size, level2book=0, coid=''):
    l2b = level2book if level2book else mi.public_client.get_product_order_book(pid, level=2)
    pricesize = getBestLimitSellInfo(mi,pid,size,l2b)
    limitaskprice_str = pricesize[0]
    limitaskvolume_str = pricesize[1]
    return mi.auth_client.sell(
        product_id=pid,
        order_type="limit",
        size=limitaskvolume_str,
        price=limitaskprice_str,
        client_oid=coid,
    )
