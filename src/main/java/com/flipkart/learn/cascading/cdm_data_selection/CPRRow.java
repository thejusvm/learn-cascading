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

    //protected String[] compoundFields;

    public CPRRow(Fields outputFields) {
        super(outputFields);
        //this.compoundFields = compoundFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();

        Tuple productPageListingAttributes = (Tuple) entry.getObject(DataFields._PRODUCTPAGELISTINGATTRIBUTES);

        //if product page view happenend then collect all the required fields
        if (productPageListingAttributes != null) {
            Integer productPageListingIndex = entry.getInteger(DataFields._PRODUCTPAGELISTINGINDEX);
//            if (productPageListingIndex == 0) { //getting only primary listing
            Tuple sessionAttributes = (Tuple) entry.getObject(DataFields._SESSIONATTRIBUTES);
            String platform = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                    DataFields._PLATFORM).get().getIdx());
            String deviceId = sessionAttributes.getString(avroSchemaReader.getIndex(DataFields._SESSIONATTRIBUTES,
                    DataFields._DEVICEID).get().getIdx());


            String findingMethod = entry.getString(DataFields._FINDINGMETHOD);

            //getting position for eda purposes
            Tuple productCardAttributes = (Tuple) entry.getObject(DataFields._PRODUCTCARDATTRIBUTES);
            Integer position;
            try{
                position = productCardAttributes.getInteger(avroSchemaReader.getIndex(
                        DataFields._PRODUCTCARDATTRIBUTES, DataFields._POSITION).get().getIdx());
            } catch (NullPointerException e){
                position = -999;
            }

            //product page attributes
            Tuple productPageAttributes = (Tuple) entry.getObject(DataFields._PRODUCTPAGEATTRIBUTES);

            String fetchId = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._FETCHID)
                            .get().getIdx());
            String ppvTimestamp = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._TIMESTAMP)
                            .get().getIdx());

            String productId = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._PRODUCTID).get().getIdx());
            String isVideoAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISVIDEOAVAILABLE)
                            .get().getIdx());
            String isImagesAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISIMAGESAVAILABLE)
                            .get().getIdx());
            String finalProductState = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._FINALPRODUCTSTATE)
                            .get().getIdx());
            String isSwatchAvailable = productPageAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._ISSWATCHAVAILABLE)
                            .get().getIdx());
            Integer ugcReviewCount = productPageAttributes.getInteger(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCREVIEWCOUNT)
                            .get().getIdx());
            Double ugcRatingBase = productPageAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCRATINGBASE)
                            .get().getIdx());
            Double ugcAvgRating = productPageAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCAVGRATING)
                            .get().getIdx());
            ugcAvgRating = ugcAvgRating / ugcRatingBase;
            Integer ugcRatingCount = productPageAttributes.getInteger(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGEATTRIBUTES, DataFields._UGCRATINGCOUNT)
                            .get().getIdx());


            //product page listing attributes
            String listingId = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._LISTINGID)
                            .get().getIdx());
            String isServiceable = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISSERVICEABLE)
                            .get().getIdx());
            String availabilityStatus = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._AVAILABILITYSTATUS)
                            .get().getIdx());
            String state = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._STATE)
                            .get().getIdx());
            String isFlipkartAdvantage = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISFLIPKARTADVANTAGE)
                            .get().getIdx());
            String deliveryDate = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._DELIVERYDATE)
                            .get().getIdx());
            String minDeliveryDateEpochMs = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._MINDELIVERYDATEEPOCHMS).get().getIdx());
            String maxDeliveryDateEpochMs = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._MAXDELIVERYDATEEPOCHMS).get().getIdx());
            Double mrp = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._MRP).get().getIdx()
            );
            Double finalPrice = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FINALPRICE)
                            .get().getIdx());
            Double fsp = productPageListingAttributes.getDouble(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._FSP)
                            .get().getIdx());
            String isCodAvailable = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._ISCODAVAILABLE)
                            .get().getIdx());
            String deliverySpeedOptions = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES,
                            DataFields._DELIVERYSPEEDOPTIONS).get().getIdx());
            String prexoOfferId = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._PREXOOFFERID)
                            .get().getIdx());
            String offerIds = productPageListingAttributes.getString(
                    avroSchemaReader.getIndex(DataFields._PRODUCTPAGELISTINGATTRIBUTES, DataFields._OFFERIDS)
                            .get().getIdx());


            //Rest other signals
            Double productPageViews = entry.getDouble(DataFields._PRODUCTPAGEVIEWS);
            //this line is shifted above because this is also a filtering signal
            // Integer productPageListingIndex = entry.getInteger(DataFields._PRODUCTPAGELISTINGINDEX);
            Double addToCartClicks = entry.getDouble(DataFields._ADDTOCARTCLICKS);
            Double buyNowClicks = entry.getDouble(DataFields._BUYNOWCLICKS);


            Tuple result = new Tuple();
            result.addAll(fetchId, ppvTimestamp, platform, deviceId, findingMethod, productId, isVideoAvailable, isImagesAvailable, finalProductState, isSwatchAvailable, ugcReviewCount,
                    ugcAvgRating, ugcRatingCount, listingId, isServiceable, availabilityStatus, state, isFlipkartAdvantage,
                    deliveryDate, minDeliveryDateEpochMs, maxDeliveryDateEpochMs, mrp, finalPrice, fsp, isCodAvailable,
                    deliverySpeedOptions, prexoOfferId, offerIds, productPageViews, productPageListingIndex,
                    addToCartClicks, buyNowClicks, position);
            if(platform.equalsIgnoreCase("AndroidApp")){
                functionCall.getOutputCollector().add(result);
            }

//            }
        }
    }
}
