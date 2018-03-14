package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.IOException;

/**
 * Created by shubhranshu.shekhar on 06/06/17.
 */
public class CPRRow extends BaseOperation implements Function {

    private static AvroSchemaReader avroSchemaReader;

    static {
        try {
            avroSchemaReader = new AvroSchemaReader("/cpr_data_schema/impressionppvSchema.avsc")
                    .buildSchema();
        }
        catch (IOException e){
            e.printStackTrace();//switch to logger for hadoop runs
        }
    }

    public String cleanString(String str){
        return str.replaceAll("\n", " ").replaceAll("\t", " ");
    }

    //protected String[] compoundFields;

    public CPRRow(Fields outputFields) {
        super(outputFields);
        //this.compoundFields = compoundFields;
    }

//    private Double getDoubleData(TupleEntry entry, String level1Key, String level2Key, double defaultVal) {
//        Tuple tuple = (Tuple) entry.getObject(level1Key);
//        if(tuple != null) {
//            Optional<AvroSchemaReader.NodeIndex> l2KeyIndex = avroSchemaReader.getIndex(level1Key, level2Key);
//            Integer l2KeyIdx = l2KeyIndex.get().getIdx();
//            double value = tuple.getDouble(l2KeyIdx);
//            return value;
//        }
//        return defaultVal;
//    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();

        Tuple productPageListingAttributes = (Tuple) entry.getObject(DataFields._PRODUCTPAGELISTINGATTRIBUTES);

        //if product page view happenend then collect all the required fields

        Integer productPageListingIndex = entry.getInteger(DataFields._PRODUCTPAGELISTINGINDEX);
//            if (productPageListingIndex == 0) { //getting only primary listing
        Tuple sessionAttributes = (Tuple) entry.getObject(DataFields._SESSIONATTRIBUTES);
        String platform = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                DataFields._PLATFORM).get().getIdx());
        String deviceId = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                DataFields._DEVICEID).get().getIdx());
        String sessionId = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                DataFields._SESSIONID).get().getIdx());
        String visitorId = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                DataFields._VISITORID).get().getIdx());


        String accoutId = entry.getString(DataFields._ACCOUNTID);
        if(accoutId == null) {
            accoutId = deviceId;
        }
        String findingMethod = entry.getString(DataFields._FINDINGMETHOD);
        String productCardImpressionsFilter = entry.getString(DataFields._PRODUCTCARDIMPRESSIONFILTER);


        String fetchId = null;
        String timestamp = null;
        String productId = null;
        String isVideoAvailable = null;
        String isImagesAvailable = null;
        String finalProductState = null;
        String isSwatchAvailable = null;
        Integer ugcReviewCount = null;
        Double ugcAvgRating = null;
        Integer ugcRatingCount = null;

        //getting position for eda purposes
        Tuple productCardAttributes = (Tuple) entry.getObject(DataFields._PRODUCTCARDATTRIBUTES);
        Integer position;
        try {
            position = productCardAttributes.getInteger(avroSchemaReader.getIndex(
                    DataFields._PRODUCTCARDATTRIBUTES, DataFields._POSITION).get().getIdx());
            productId = productCardAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTCARDATTRIBUTES, DataFields._PRODUCTID).get().getIdx());
            timestamp = productCardAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTCARDATTRIBUTES, DataFields._TIMESTAMP).get().getIdx());
        } catch (NullPointerException e) {
            position = -999;
        }

        //product page attributes
        Tuple productPageAttributes = (Tuple) entry.getObject(DataFields._PRODUCTPAGEATTRIBUTES);

        if (productPageAttributes != null) {

            fetchId = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._FETCHID)
                            .get().getIdx());
//            timestamp = productPageAttributes.getString(
//                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._TIMESTAMP)
//                            .get().getIdx());

            productId = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._PRODUCTID).get().getIdx());
            isVideoAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISVIDEOAVAILABLE)
                            .get().getIdx());
            isImagesAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISIMAGESAVAILABLE)
                            .get().getIdx());
            finalProductState = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._FINALPRODUCTSTATE)
                            .get().getIdx());
            isSwatchAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISSWATCHAVAILABLE)
                            .get().getIdx());
            ugcReviewCount = productPageAttributes.getInteger(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCREVIEWCOUNT)
                            .get().getIdx());
            Double ugcRatingBase = productPageAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCRATINGBASE)
                            .get().getIdx());
            ugcAvgRating = productPageAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCAVGRATING)
                            .get().getIdx());
            ugcAvgRating = ugcAvgRating / ugcRatingBase;
            ugcRatingCount = productPageAttributes.getInteger(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCRATINGCOUNT)
                            .get().getIdx());

        }

        //search attributes
        Tuple searchAttributes = (Tuple) entry.getObject(DataFields._SEARCHATTRIBUTES);
        String sqId = null;
        String searchQuery = null;
        if(searchAttributes != null) {
            sqId = searchAttributes.getString(avroSchemaReader.getIndex(DataFields._SEARCHATTRIBUTES, DataFields._SEARCHQUERYID).get().getIdx());
            searchQuery = cleanString(searchAttributes.getString(avroSchemaReader.getIndex(DataFields._SEARCHATTRIBUTES, DataFields._ORIGINALSEARCHQUERY).get().getIdx()));
        } else {
//            sqId = fetchId;
        }

        //product page listing attributes
        String listingId = null;
        String isServiceable = null;
        String availabilityStatus = null;
        String state = null;
        String isFlipkartAdvantage = null;
        String isCodAvailable = null;
        Double fsp = null;
        Double finalPrice = null;
        Double mrp = null;
        String maxDeliveryDateEpochMs = null;
        String minDeliveryDateEpochMs = null;
        String deliveryDate = null;
        String deliverySpeedOptions = null;
        String prexoOfferId = null;
        String offerIds = null;
        if (productPageListingAttributes != null) {
            listingId = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._LISTINGID)
                            .get().getIdx());
            isServiceable = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISSERVICEABLE)
                            .get().getIdx());
            availabilityStatus = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._AVAILABILITYSTATUS)
                            .get().getIdx());
            state = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._STATE)
                            .get().getIdx());
            isFlipkartAdvantage = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISFLIPKARTADVANTAGE)
                            .get().getIdx());
            deliveryDate = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._DELIVERYDATE)
                            .get().getIdx());
            minDeliveryDateEpochMs = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._MINDELIVERYDATEEPOCHMS).get().getIdx());
            maxDeliveryDateEpochMs = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._MAXDELIVERYDATEEPOCHMS).get().getIdx());
            mrp = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._MRP).get().getIdx()
            );
            finalPrice = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FINALPRICE)
                            .get().getIdx());
            fsp = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FSP)
                            .get().getIdx());
            isCodAvailable = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISCODAVAILABLE)
                            .get().getIdx());
            deliverySpeedOptions = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._DELIVERYSPEEDOPTIONS).get().getIdx());
            prexoOfferId = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._PREXOOFFERID)
                            .get().getIdx());
            offerIds = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._OFFERIDS)
                            .get().getIdx());
        }


        Tuple productCardListingAttributes = (Tuple) entry.getObject(DataFields._PRODUCTCARDLISTINGATTRIBUTES);
        if(productCardListingAttributes != null) {
            if(mrp == null) {
                mrp = productCardListingAttributes.getDouble(avroSchemaReader.getIndex(
                        DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._MRP).get().getIdx());
            }
            if(fsp == null) {
                fsp = productCardListingAttributes.getDouble(avroSchemaReader.getIndex(
                        DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FSP).get().getIdx());
            }
            if(finalPrice == null) {
                finalPrice = productCardListingAttributes.getDouble(avroSchemaReader.getIndex(
                        DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FINALPRICE).get().getIdx());
            }
        }

        //Rest other signals
        Double productCardClicks = entry.getDouble(DataFields._PRODUCTCARDCLICKS);
        Double productPageViews = entry.getDouble(DataFields._PRODUCTPAGEVIEWS);
        //this line is shifted above because this is also a filtering signal
        // Integer productPageListingIndex = entry.getInteger(DataFields._PRODUCTPAGELISTINGINDEX);
        Double addToCartClicks = entry.getDouble(DataFields._ADDTOCARTCLICKS);
        Double buyNowClicks = entry.getDouble(DataFields._BUYNOWCLICKS);

        double discountPrice = 0;
        double discountPercent = 0;
        if(fsp != null && finalPrice != null) {
            discountPrice = fsp - finalPrice;
            discountPercent = (fsp - finalPrice) * 100 / fsp ;
        }

        if(productId != null) {

            Tuple result = new Tuple();
            result.addAll(sessionId, accoutId, visitorId, fetchId, timestamp, platform, deviceId, findingMethod, sqId, searchQuery, productId, isVideoAvailable, isImagesAvailable, finalProductState, isSwatchAvailable, ugcReviewCount,
                    ugcAvgRating, ugcRatingCount, listingId, isServiceable, availabilityStatus, state, isFlipkartAdvantage,
                    deliveryDate, minDeliveryDateEpochMs, maxDeliveryDateEpochMs, mrp, finalPrice, fsp, discountPrice, discountPercent, isCodAvailable,
                    deliverySpeedOptions, prexoOfferId, offerIds, productCardClicks, productPageViews, productPageListingIndex,
                    addToCartClicks, buyNowClicks, position, productCardImpressionsFilter);
            functionCall.getOutputCollector().add(result);
        }

    }
}
