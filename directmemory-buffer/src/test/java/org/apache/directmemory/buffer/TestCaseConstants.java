package org.apache.directmemory.buffer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.impl.ByteBufferUnpooledPartition;
import org.apache.directmemory.buffer.impl.UnsafePooledPartition;
import org.apache.directmemory.buffer.impl.UnsafeUnpooledPartition;
import org.apache.directmemory.buffer.selector.ProcessorLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.selector.RoundRobinPartitionSliceSelector;
import org.apache.directmemory.buffer.selector.ThreadLocalPartitionSliceSelector;


public class TestCaseConstants
{
    public static final Object[] PARTITION_FACTORIES =
        new Object[] { ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY,
            ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY,
            UnsafePooledPartition.UNSAFE_PARTITION_FACTORY,
            ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY,
            ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY,
            UnsafeUnpooledPartition.UNSAFE_PARTITION_FACTORY };

    public static final Object[] PARTITION_SLICE_SELECTORS = new Object[] { RoundRobinPartitionSliceSelector.class,
        ThreadLocalPartitionSliceSelector.class, ProcessorLocalPartitionSliceSelector.class };

    public static final Collection<Object[]> EXECUTION_PARAMETER_MUTATIONS = buildExecutionParameterMutations();

    public static String buildPartitionFactoryName( Object partitionFactory )
    {
        String partitionFactoryName = partitionFactory.getClass().getEnclosingClass().getSimpleName();
        if ( partitionFactory == ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY
            || partitionFactory == ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY )
        {
            partitionFactoryName += "{Direct}";
        }
        else if ( partitionFactory == ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY
            || partitionFactory == ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY )
        {
            partitionFactoryName += "{Heap}";
        }
        return partitionFactoryName;
    }

    private static List<Object[]> buildExecutionParameterMutations()
    {
        List<Object[]> mutations = new LinkedList<Object[]>();

        for ( Object partitionFactory : PARTITION_FACTORIES )
        {
            for ( Object partitionSliceSelector : PARTITION_SLICE_SELECTORS )
            {
                String partitionFactoryName = buildPartitionFactoryName( partitionFactory );
                mutations.add( new Object[] { partitionFactoryName,
                    ( (Class<?>) partitionSliceSelector ).getSimpleName(), partitionFactory, partitionSliceSelector } );
            }
        }

        return mutations;
    }

}
