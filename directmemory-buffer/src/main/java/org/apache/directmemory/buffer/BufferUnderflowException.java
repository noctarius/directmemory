package org.apache.directmemory.buffer;

public class BufferUnderflowException
    extends RuntimeException
{

    private static final long serialVersionUID = -2206747907555711373L;

    public BufferUnderflowException()
    {
        super();
    }

    public BufferUnderflowException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public BufferUnderflowException( String message )
    {
        super( message );
    }

    public BufferUnderflowException( Throwable cause )
    {
        super( cause );
    }

}
