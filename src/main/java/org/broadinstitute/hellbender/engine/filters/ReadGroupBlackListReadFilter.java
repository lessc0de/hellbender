package org.broadinstitute.hellbender.engine.filters;

import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTag;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.text.XReadLines;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Keep records not matching the read group tag and exact match string.
 * For example, this filter value:
 *   PU:1000G-mpimg-080821-1_1
 * would filter out a read with the read group PU:1000G-mpimg-080821-1_1
 */
public final class ReadGroupBlackListReadFilter implements ReadFilter {
    public static final String COMMENT_START = "#";
    public static final String FILTER_ENTRY_SEPARATOR = ":";

    private Set<Entry<String, Collection<String>>> blacklistEntries;

    /**
     * Creates a filter using the lists of files with blacklisted read groups.
     * Any entry can be a path to a file (ending with "list" or "txt" which
     * will load blacklist from that file. This scheme works recursively
     * (ie the file may contain names of further files etc).
     */
    public ReadGroupBlackListReadFilter(final List<String> blackLists) {
            final Map<String, Collection<String>> filters = new TreeMap<>();
            for (String blackList : blackLists) {
                try {
                    addFilter(filters, blackList, null, 0);
                } catch (IOException e) {
                    throw new UserException("Incorrect blacklist:" + blackList, e);
                }
            }
            this.blacklistEntries = filters.entrySet();
    }

    private void addFilter(final Map<String, Collection<String>> filters, final String filter, final File parentFile, final int parentLineNum) throws IOException {
        if (filter.toLowerCase().endsWith(".list") || filter.toLowerCase().endsWith(".txt")) {
            addFiltersFromFile(filters, filter);
        } else {
            addFiltersFromString(filters, filter, parentFile, parentLineNum);
        }
    }

    private void addFiltersFromString(final Map<String, Collection<String>> filters, final String filter, final File parentFile, final int parentLineNum) {
        final String[] split = filter.split(FILTER_ENTRY_SEPARATOR, 2);
        checkValidFilterEntry(filter, parentFile, parentLineNum, split);

        //Note: if we're here, we know that split has exactly 2 elements.
        filters.computeIfAbsent(split[0], k -> new TreeSet<>()).add(split[1]);
    }

    private void checkValidFilterEntry(String filter, File parentFile, int parentLineNum, String[] split) {
        String message = null;
        if (split.length != 2) {
            message = "Invalid read group filter: " + filter;
        } else if (split[0].length() != 2) {
            message = "Tag is not two characters: " + filter;
        }

        if (message != null) {
            if (parentFile != null) {
                message += ", " + parentFile.getAbsolutePath() + ":" + parentLineNum;
            }
            message += ", format is <TAG>:<SUBSTRING>";
            throw new UserException(message);
        }
    }

    private void addFiltersFromFile(final Map<String, Collection<String>> filters, final String fileName) throws IOException {
        final File file = new File(fileName);
        try (final XReadLines lines = new XReadLines(file)) {
            int lineNum = 0;
            for (String line : lines) {
                lineNum++;
                if (!line.trim().isEmpty() && !line.startsWith(COMMENT_START)) {
                    addFilter(filters, line, file, lineNum);
                }
            }
        }
    }

    @Override
    public boolean test(final SAMRecord read) {
        final SAMReadGroupRecord readGroup = read.getReadGroup();
        if (readGroup == null){
            return true;
        }

        for (final Entry<String, Collection<String>> blacklistEntry : blacklistEntries) {
            final String attributeType = blacklistEntry.getKey();

            final String attribute;
            if (SAMReadGroupRecord.READ_GROUP_ID_TAG.equals(attributeType) || SAMTag.RG.name().equals(attributeType)) {
                attribute = readGroup.getId();
            } else {
                attribute = readGroup.getAttribute(attributeType);
            }
            if (attribute != null && blacklistEntry.getValue().contains(attribute)) {
                return false;
            }
        }

        return true;
    }

}
