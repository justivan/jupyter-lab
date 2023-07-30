from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import io


class HotelDataReadCsv(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            return pd.read_csv(
                io.StringIO(X),
                usecols=[
                    "HotelID",
                    "HotelName",
                    "Country",
                    "State",
                    "City",
                    "HotelTypeID",
                    "Longitude",
                    "Latitude",
                    "Giata",
                    "SaleMail",
                    "CreateDate",
                    "LastChangeDate",
                    "IsActive",
                ],
                dtype={
                    "HotelID": int,
                    "HotelName": str,
                    "Country": str,
                    "State": str,
                    "City": str,
                    "HotelTypeID": pd.Int64Dtype(),
                    "Longitude": float,
                    "Latitude": float,
                    "Giata": pd.Int64Dtype(),
                    "SaleMail": str,
                    "IsActive": str,
                },
                parse_dates=[
                    "CreateDate",
                    "LastChangeDate",
                ],
                date_format="%d-%b-%y",
            )


class HotelDataEncoder(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.exclude = [
            1,  # NO ACCOMMODATION
            2,  # FLIGHT ONLY PAX
            3,  # TOUR ONLY PAX
            4,  # Transfer Only Pax
            5,  # SPLIT BOOKINGS
            5000,  # TEST
            5005,  # TEST HOTEL
            90001,  # HOTEL SHOP RESERVATIONS
            191680,  # ROULETTE OFFER
            202356,  # TEST HOTEL
            203441,  # ROULETTE HOTEL RAK
            209384,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            100,  # HOTEL SHOP RESERVATIONS
            90018,  # HOTEL SHOP RESERVATIONS
            209385,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            217636,  # TEST HOTEL
            218648,  # CRUISE
            218736,  # TEST HOTEL
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            X.drop(X[X["HotelID"].isin(self.exclude)].index, inplace=True)
            X.rename(columns = {'HotelName':'HotelName_GWG'}, inplace=True)
            X["HotelName_GWG"] = X["HotelName_GWG"].str.split().str.join(" ")
            X["HotelName"] = X["HotelName_GWG"].str.split("(").str[0]
            X["HotelName"] = X["HotelName"].str.replace("-", " ", regex=False)
            X["HotelName"] = X["HotelName"].str.replace("+", " ", regex=False)
            X["HotelName"] = X["HotelName"].str.replace(" AND ", " & ", regex=False)
            X["HotelName"] = X["HotelName"].str.split().str.join(" ")
            X["SaleMail"] = X["SaleMail"].fillna("undefined").str.lower()
            X.sort_values(["Country", "HotelName"], inplace=True)
            
            return X
        return None