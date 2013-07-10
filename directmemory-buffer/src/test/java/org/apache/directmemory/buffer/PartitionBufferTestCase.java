package org.apache.directmemory.buffer;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.impl.BufferUtils;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
public class PartitionBufferTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    public PartitionBufferTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                    Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
    }

    @Test
    public void testAllocation()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "512K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < 1024 * 1024 + 1; i++ )
            {
                partitionBuffer.writeByte( 1 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 3, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 2 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testAllocation2()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "256K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                partitionBuffer.writeByte( 1 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
            }

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test( expected = RuntimeException.class )
    public void testAllocationBufferFull()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "1M", 1, "256K" );

        try
        {
            // Unpooled pools will never overflow but can throw OOM
            if ( !pool.isPooled() )
            {
                throw new RuntimeException( "Unpooled pools will never overflow but can throw OOM" );
            }

            for ( int o = 0; o < 100; o++ )
            {
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

                PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    partitionBuffer.writeByte( 1 );
                }
                partitionBuffer.flush();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testAllocationFullRound()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "10M", 5, "256K" );

        try
        {
            for ( int o = 0; o < 10; o++ )
            {
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

                PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    partitionBuffer.writeByte( 1 );
                }
                partitionBuffer.flush();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

}
