package com.flipkart.learn.cascading.commons.cascading.subAssembly;


import java.beans.ConstructorProperties;
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/** Class Average is an {@link Aggregator} that returns the average of all numeric values in the current group. */
public class AverageIgnoreZero extends BaseOperation<AverageIgnoreZero.Context> implements Aggregator<AverageIgnoreZero.Context>
{
    /** Field FIELD_NAME */
    public static final String FIELD_NAME = "average";

    /** Field type */
    private Type type = Double.class;
    private CoercibleType canonical;

    /** Class Context is used to hold intermediate values. */
    protected static class Context
    {
        private final CoercibleType canonical;

        Tuple tuple = Tuple.size( 1 );
        double sum = 0.0D;
        long count = 0L;

        public Context( CoercibleType canonical )
        {
            this.canonical = canonical;
        }

        public Context reset()
        {
            sum = 0.0D;
            count = 0L;

            return this;
        }

        public Tuple result()
        {
            tuple.set( 0, canonical.canonical( sum / count ) );

            return tuple;
        }
    }

    /** Constructs a new instance that returns the average of the values encountered in the field name "average". */
    public AverageIgnoreZero()
    {
        super( 1, new Fields( FIELD_NAME, Double.class ) );

        this.canonical = Coercions.coercibleTypeFor( this.type );
    }

    /**
     * Constructs a new instance that returns the average of the values encountered in the given fieldDeclaration field name.
     *
     * @param fieldDeclaration of type Fields
     */
    @ConstructorProperties({"fieldDeclaration"})
    public AverageIgnoreZero( Fields fieldDeclaration )
    {
        super( 1, fieldDeclaration );

        if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
            throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );

        if( fieldDeclaration.hasTypes() )
            this.type = fieldDeclaration.getType( 0 );

        this.canonical = Coercions.coercibleTypeFor( this.type );
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
        operationCall.setContext( new Context( canonical ) );
    }

    @Override
    public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
        aggregatorCall.getContext().reset();
    }

    @Override
    public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
        Context context = aggregatorCall.getContext();
        TupleEntry arguments = aggregatorCall.getArguments();

        double doubleVal = arguments.getDouble(0);
        if(doubleVal != 0) {
            context.sum += doubleVal;
            context.count += 1L;
        }
    }

    @Override
    public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
        aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

    private Tuple getResult( AggregatorCall<Context> aggregatorCall )
    {
        return aggregatorCall.getContext().result();
    }
}

