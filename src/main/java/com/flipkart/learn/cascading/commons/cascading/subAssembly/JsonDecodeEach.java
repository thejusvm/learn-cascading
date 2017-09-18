package com.flipkart.learn.cascading.commons.cascading.subAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.JsonDecoder;
import com.flipkart.learn.cascading.commons.cascading.SerializableTypeReference;

/**
 * Created by thejus on 20/11/15.
 */
public class JsonDecodeEach extends SubAssembly {

    public JsonDecodeEach(Pipe basePipe, Fields argumentSelector, Class clazz) {
        basePipe = new Each(basePipe, argumentSelector, new JsonDecoder(argumentSelector, clazz), Fields.SWAP);
        setTails(basePipe);
    }

    public JsonDecodeEach(Pipe basePipe, Fields argumentSelector, SerializableTypeReference typeReference) {
        basePipe = new Each(basePipe, argumentSelector, new JsonDecoder(argumentSelector, typeReference), Fields.SWAP);
        setTails(basePipe);
    }
}
