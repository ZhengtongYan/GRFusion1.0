/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
// This file is created by LX for debugging
package org.hsqldb_voltpatches;


/**
 * This file isn't long for this world. It's just something I've been using
 * to debug multi-process rejoin stuff.
 *
 */
public class HSQLLog {
    // Add LX
    public static boolean GDebug = true;
    public static void GLog(String className, int lineNo, String message)
    {
        if(HSQLLog.GDebug)
        {
            System.out.println("#HSQLDebug: " + className + "@" + Integer.toString(lineNo) + " => " + message);
        }
    }
    // End LX
}
