package com.flipkart.learn.cascading.commons.cascading.subAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.JsonEncoder;

/**
 * Created by thejus on 20/11/15.
 */
public class JsonEncodeEach extends SubAssembly {

    public JsonEncodeEach(Pipe basePipe, Fields argumentSelector) {
        basePipe = new Each(basePipe, argumentSelector, new JsonEncoder(argumentSelector), Fields.SWAP);
        setTails(basePipe);
    }
}
