package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.tuple.Fields;

/**
 * Created by shubhranshu.shekhar on 13/06/17.
 */
public class DataFields {
    //session attributes
    public static final String _SESSIONATTRIBUTES = "sessionAttributes";
    public static final String _PLATFORM = "platform";
    public static final String _DEVICEID = "uaDeviceId";
    public static final String _SESSIONID = "sessionId";
    public static final String _ACCOUNTID = "accountId";
    public static final String _VISITORID = "visitorId";

    //finding method
    public static final String _FINDINGMETHOD = "findingMethod";

    //product attribute related
    public static final String _CMS = "cms";
    public static final String _FSN = "fsn";
    public static final String _VERTICAL = "vertical";
    public static final String _BRAND = "brand";

    public static final String _PRODUCTCARDLISTINGATTRIBUTES = "productCardListingAttributes";

    //product card attributes
    public static final String _PRODUCTCARDATTRIBUTES = "productCardAttributes";
    public static final String _POSITION = "position";

    //product page attributestimestamp
    public static final String _PRODUCTPAGEATTRIBUTES = "productPageAttributes";
    public static final String _FETCHID = "fetchId";
    public static final String _TIMESTAMP = "timestamp";
    public static final String _PRODUCTID = "productId";
    public static final String _ANALYTICALSUPERCATEGORY = "analyticalSuperCategory";
    public static final String _ANALYTICALCATEGORY = "analyticalCategory";
    public static final String _ANALYTICALSUBCATEGORY = "analyticalSubCategory";
    public static final String _ANALYTICALVERTICAL = "analyticalVertical";
    public static final String _ISVIDEOAVAILABLE = "isVideoAvailable";
    public static final String _ISIMAGESAVAILABLE = "isImagesAvailable";
    public static final String _FINALPRODUCTSTATE = "finalProductState";
    public static final String _ISVISUALDISCOVERENABLED = "isVisualDiscoverEnabled";
    public static final String _ISDIGITAL = "isDigital";
    public static final String _ISSWATCHAVAILABLE = "isSwatchAvailable";
    public static final String _UGCREVIEWCOUNT = "ugcReviewCount";
    public static final String _UGCRATINGBASE = "ugcRatingBase";
    public static final String _UGCAVGRATING = "ugcAvgRating";
    public static final String _UGCRATINGCOUNT = "ugcRatingCount";

    //Search attributes
    public static final String _SEARCHATTRIBUTES = "searchAttributes";
    public static final String _SEARCHQUERYID = "searchQueryId";
    public static final String _ORIGINALSEARCHQUERY = "originalSearchQuery";

    //product page listing attributes
    public static final String _PRODUCTPAGELISTINGATTRIBUTES = "productPageListingAttributes";//parent node
    public static final String _LISTINGID = "listingId";
    public static final String _ISSERVICEABLE = "isServiceable";
    public static final String _AVAILABILITYSTATUS = "availabilityStatus";
    public static final String _STATE = "state";
    public static final String _ISFLIPKARTADVANTAGE = "isFlipkartAdvantage";
    public static final String _DELIVERYDATE = "deliveryDate";
    public static final String _MINDELIVERYDATEEPOCHMS = "minDeliveryDateEpochMs";
    public static final String _MAXDELIVERYDATEEPOCHMS = "maxDeliveryDateEpochMs";
    public static final String _MRP = "mrp";
    public static final String _FINALPRICE = "finalPrice";
    public static final String _FSP = "fsp";
    public static final String _SELLERID = "sellerId";
    public static final String _ISCODAVAILABLE = "isCodAvailable";
    public static final String _DELIVERYSPEEDOPTIONS = "deliverySpeedOptions";
    public static final String _PREXOOFFERID = "prexoOfferId";
    public static final String _OFFERIDS = "offerIds";

    //rest others
    public static final String _PRODUCTCARDCLICKS ="productCardClicks";
    public static final String _FIRSTPRODUCTCARDCLICKTIMESTAMP = "firstProductCardClickTimestamp";
    public static final String _PRODUCTPAGEVIEWS = "productPageViews";
    public static final String _ADDTOCARTCLICKS = "addToCartClicks";
    public static final String _FIRSTADDTOCARTCLICKTIMESTAMP = "firstAddToCartClickTimestamp";
    public static final String _BUYNOWCLICKS = "buyNowClicks";
    public static final String _FIRSTBUYNOWCLICKTIMESTAMP = "firstBuyNowClickTimestamp";
    public static final String _PRODUCTCARDIMPRESSIONFILTER = "productCardImpressionFilter";
    public static final String _PRODUCTPAGELISTINGINDEX = "productPageListingIndex";
    public static final String _BUYINTENT = "buyIntent";


    //Final output of cdm data for cpr--required because of the custom operation required to fetch desired fields

    public static final Fields cdmOutputFields = new Fields(
            _SESSIONID,
            _ACCOUNTID,
            _VISITORID,
            _FETCHID,
            _TIMESTAMP,
            _PLATFORM,
            _DEVICEID,
            _FINDINGMETHOD,
            _SEARCHQUERYID,
            _ORIGINALSEARCHQUERY,
            _PRODUCTID,
            _ISVIDEOAVAILABLE,
            _ISIMAGESAVAILABLE,
            _FINALPRODUCTSTATE,
            _ISSWATCHAVAILABLE,
            _UGCREVIEWCOUNT,
            _UGCAVGRATING,
            _UGCRATINGCOUNT,
            _LISTINGID,
            _ISSERVICEABLE,
            _AVAILABILITYSTATUS,
            _STATE,
            _ISFLIPKARTADVANTAGE,
            _DELIVERYDATE,
            _MINDELIVERYDATEEPOCHMS,
            _MAXDELIVERYDATEEPOCHMS,
            _MRP,
            _FINALPRICE,
            _FSP,
            _ISCODAVAILABLE,
            _DELIVERYSPEEDOPTIONS,
            _PREXOOFFERID,
            _OFFERIDS,
            _PRODUCTCARDCLICKS,
            _PRODUCTPAGEVIEWS,
            _PRODUCTPAGELISTINGINDEX,
            _ADDTOCARTCLICKS,
            _BUYNOWCLICKS,
            _POSITION,
            _PRODUCTCARDIMPRESSIONFILTER);
}
