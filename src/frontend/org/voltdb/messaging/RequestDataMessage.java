/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.iv2.InitiatorMailbox;

public class RequestDataMessage extends TransactionInfoBaseMessage {

    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    private InitiatorMailbox m_sender;

    private long m_destinationHSId;

    ArrayList<Long> m_requestDestinations = new ArrayList<Long>();

    // Empty constructor for de-serialization
    RequestDataMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataMessage(long sourceHSId,
            long destinationHSId,
            long txnId,
            long uniqueId,
            boolean isReadOnly,
            boolean isForReplay) {
        super(sourceHSId, destinationHSId, txnId, uniqueId, isReadOnly, isForReplay);
        m_destinationHSId = destinationHSId;
        m_subject = Subject.DEFAULT.getId();
    }

    public RequestDataMessage(long sourceHSId, long destinationHSId, RequestDataMessage msg) {
        super(sourceHSId, destinationHSId, msg.m_txnId, msg.m_uniqueId, msg.m_isReadOnly, msg.m_isForReplay);
        m_destinationHSId = destinationHSId;
        m_subject = Subject.DEFAULT.getId();
    }

    // getters and setters
    // request destination is the destination of the message.
    // a set of destinations (for chained messaging) is accessed as a stack.
    public void pushRequestDestination(long destHSId) {
        m_requestDestinations.add(destHSId);
    }

    public long popRequestDestination() {
        return m_requestDestinations.remove(m_requestDestinations.size()-1);
    }

    public long getRequestDestination() {
        return m_requestDestinations.get(m_requestDestinations.size()-1);
    }

    public void setRequestDestinations(ArrayList<Long> dest) {
        m_requestDestinations = dest;
    }

    public ArrayList<Long> getRequestDestinations() {
        return m_requestDestinations;
    }

    // destination site ID
    public long getDestinationSiteId() {
        return m_destinationHSId;
    }

    public void setDestinationSiteId(long destHSId) {
        m_destinationHSId = destHSId;
    }

    // an original sender
    public void setSender(InitiatorMailbox sender) {
        m_sender = sender;
    }

    public InitiatorMailbox getSender() {
        return m_sender;
    }

    @Override
    public int getSerializedSize()
    {
        return super.getSerializedSize();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
System.out.println("REQUEST " + buf.capacity() + ", " + buf.position());
        buf.put(VoltDbMessageFactory.REQUEST_DATA_ID);
        super.flattenToBuffer(buf);
/*
        buf.putLong(m_destinationHSId);

        for (int i=0; i<m_requestDestinations.size(); i++)
        {
            buf.putLong(m_requestDestinations.get(i));
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
        */
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {

        super.initFromBuffer(buf);
/*
        m_destinationHSId = buf.getLong();

        for (int i=0; i<m_requestDestinations.size(); i++)
        {
            m_requestDestinations.add(buf.getLong());
        }
        */
    }

    // Add LX To fix the abstract class error
    public  void toDuplicateCounterString(StringBuilder sb){
        return ;
    }
    // End LX
}