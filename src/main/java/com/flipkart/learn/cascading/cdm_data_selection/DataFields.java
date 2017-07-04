package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.tuple.Fields;

/**
 * Created by shubhranshu.shekhar on 13/06/17.
 */
public class DataFields {
    //session attributes
    public static final String _SESSIONATTRIBUTES = "sessionAttributes";
    public static final String _PLATFORM = "platform";

    //finding method
    public static final String _FINDINGMETHOD = "findingMethod";
    public static final String _CMS = "cms";
    public static final String _FSN = "fsn";
    public static final String _VERTICAL = "vertical";

    //product page attributes
    public static String _PRODUCTPAGEATTRIBUTES = "productPageAttributes";
    public static String _TIMESTAMP = "timestamp";
    public static String _PRODUCTID = "productId";
    public static String _ANALYTICALSUPERCATEGORY = "analyticalSuperCategory";
    public static String _ANALYTICALCATEGORY = "analyticalCategory";
    public static String _ANALYTICALSUBCATEGORY = "analyticalSubCategory";
    public static String _ANALYTICALVERTICAL = "analyticalVertical";
    public static String _ISVIDEOAVAILABLE = "isVideoAvailable";
    public static String _ISIMAGESAVAILABLE = "isImagesAvailable";
    public static String _FINALPRODUCTSTATE = "finalProductState";
    public static String _ISVISUALDISCOVERENABLED = "isVisualDiscoverEnabled";
    public static String _ISDIGITAL = "isDigital";
    public static String _ISSWATCHAVAILABLE = "isSwatchAvailable";
    public static String _UGCREVIEWCOUNT = "ugcReviewCount";
    public static String _UGCRATINGBASE = "ugcRatingBase";
    public static String _UGCAVGRATING = "ugcAvgRating";
    public static String _UGCRATINGCOUNT = "ugcRatingCount";


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
    
    //Final output of cdm data for cpr--required because of the custom operation required to fetch desired fields

    public static final Fields cdmOutputFields = new Fields(_PLATFORM,
            _FINDINGMETHOD,
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
            _PRODUCTPAGEVIEWS,
            _PRODUCTPAGELISTINGINDEX,
            _ADDTOCARTCLICKS,
            _BUYNOWCLICKS);
}
