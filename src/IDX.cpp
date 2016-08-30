/**
 * Copyright (c) 2015, Hewlett-Packard Development Co., L.P.
 */
/**
 * Flex Table IDX parser
 */


#include "FlexTable.h"
#include "IDX.h"

namespace flextable
{

const char *strnstr(const char *haystack, const char *needle, size_t len)
{
    int i;
    size_t needle_len;

    /* segfault here if needle is not NULL terminated */
    if (0 == (needle_len = strlen(needle))) return haystack;

    /* Limit the search if haystack is shorter than 'len' */
    len = strnlen(haystack, len);

    for (i=0; i<(int)(len-needle_len); i++)
    {
        if ((haystack[0] == needle[0]) &&
            (0 == strncmp(haystack, needle, needle_len)))
        {
            return (char *)haystack;
        }
        haystack++;
    }
    return NULL;
}

/// BEGIN class FlexTableIDXParser
FlexTableIDXParser::FlexTableIDXParser(bool sc)
    : dre_start(-1), dre_end(-1),
      key_start(-1), key_end(-1),
      val_start(-1), val_end(-1), entry_end(-1),
      map_col_index(-1), map_data_buf(NULL), map_data_buf_len(-1),
      pair_buf(NULL), map_writer(0),
      is_flextable(false),
      storeContent(sc),
      isProcessingRecord(false)
{
}

FlexTableIDXParser::~FlexTableIDXParser() {
    for (ColLkup::iterator it = real_col_lookup.begin(); it != real_col_lookup.end(); ++it) {
        if (it->first.ptr) {
            delete[] it->first.ptr;
        }
    }
}

bool FlexTableIDXParser::fetchNextDRETag()
{
    dre_start = dre_end = -1;

    size_t reserved;// Amount of data we have to work with
    size_t reservationRequest = 256; // Amount we will request to work with
    const char* ptr; // Where we are looking
    size_t position = 0; // Current offset from getDataPtr()

    const char *search = "#DRE"; const size_t searchlen = strlen(search);

    do {
        reserved = cr.reserve(reservationRequest);
        ptr = (char*)cr.getDataPtr() + position;
        const char *eob = static_cast<const char *>(cr.getDataPtr()) + reserved;

        const char *hit = strnstr(ptr, search, reserved);
        if (hit)
        {
            dre_start = hit - static_cast<const char *>(cr.getDataPtr());
            ptr = hit+searchlen;
            while (ptr < eob)
            {
                if (*ptr == ' ' || *ptr == '\n' || *ptr == '\r' || *ptr == '?')
                {
                    dre_end = ptr - static_cast<const char *>(cr.getDataPtr());
                    return true;
                }
                ++ptr;
            }

            // We will end-up redoing the strnstr, but with more of the #dre to look at
        }
        else
        {
            // No need to search the first batch of chars
            if (reserved > searchlen)
            {
                position = reserved - searchlen;
            }
        }

        reservationRequest *= 2;  // Request twice as much data next time
    } while (!cr.noMoreData());  // Stop if we run out of data;

    if (dre_start >=0)
    {
        // EOF hit
        dre_end = reserved;
        return true;
    }

    return false;
}

bool FlexTableIDXParser::fetchThisDRE(bool quoted)
{
    entry_end = -1;

    size_t reserved;// Amount of data we have to work with
    size_t reservationRequest = 256; // Amount we will request to work with
    const char* ptr; // Where we are looking
    size_t position = dre_end; // Current offset from getDataPtr()

    // We are searching either for \n#DRE or "\n#DRE if quoted; doesn't hurt to allow \r too
    const char *search1 = quoted ? "\"\r\n#DRE" : "\r\n#DRE"; const size_t searchlen1 = strlen(search1);
    const char *search2 = quoted ? "\"\n#DRE" : "\n#DRE"; //const size_t searchlen2 = strlen(search2);
    const char *search3 = quoted ? "\"\r#DRE" : "\r#DRE"; //const size_t searchlen3 = strlen(search3);

    do {
        reserved = cr.reserve(reservationRequest);
        ptr = (char*)cr.getDataPtr() + position;

        const char *hit = strnstr(ptr, search1, reserved);
        if (!hit) hit = strnstr(ptr, search2, reserved);
        if (!hit) hit = strnstr(ptr, search3, reserved);

        if (hit)
        {
            entry_end = strnstr(hit, "#DRE", reserved-(hit-ptr)) - static_cast<const char *>(cr.getDataPtr());
            return true;
        }
        else
        {
            // No need to search the first batch of chars
            if (reserved > searchlen1)
            {
                position = reserved - searchlen1;
            }
        }

        reservationRequest *= 2;  // Request twice as much data next time
    } while (!cr.noMoreData());  // Stop if we run out of data;

    entry_end = reserved;
    return true;
}

bool FlexTableIDXParser::skipThisDRE(bool quoted)
{
    cr.seek(dre_end);
    dre_start = dre_end = entry_end = -1;

    size_t reserved;// Amount of data we have to work with
    size_t reservationRequest = 256; // Amount we will request to work with
    const char* ptr; // Where we are looking

    // We are searching either for \n#DRE or "\n#DRE if quoted; doesn't hurt to allow \r too
    const char *search1 = quoted ? "\"\r\n#DRE" : "\r\n#DRE"; const size_t searchlen1 = strlen(search1);
    const char *search2 = quoted ? "\"\n#DRE" : "\n#DRE"; //const size_t searchlen2 = strlen(search2);
    const char *search3 = quoted ? "\"\r#DRE" : "\r#DRE"; //const size_t searchlen3 = strlen(search3);

    do {
        reserved = cr.reserve(reservationRequest);
        ptr = (char*)cr.getDataPtr();

        const char *hit = strnstr(ptr, search1, reserved);
        if (!hit) hit = strnstr(ptr, search2, reserved);
        if (!hit) hit = strnstr(ptr, search3, reserved);

        if (hit)
        {
            entry_end = strnstr(hit, "#DRE", reserved-(hit-ptr)) - static_cast<const char *>(cr.getDataPtr());
            return true;
        }

        cr.seek(reserved - searchlen1);
        if (reservationRequest < 8192)
        {
            reservationRequest *= 2;  // Can expand, but no need to get huge because we're discarding stuff
        }
    } while (!cr.noMoreData() && reserved > 10);  // Stop if we run out of data;

    return true;
}

bool FlexTableIDXParser::parseThisDRE(bool fieldname, bool quoted, bool multiline)
{
    const char *ptr = (const char*)cr.getDataPtr();

    // If there's a field name expected, forward scan for it
    if (fieldname)
    {
        ssize_t pos = dre_end;
        // First nonwhite is start of name
        while (pos != entry_end)
        {
            if (ptr[pos] == '\n' || ptr[pos] == '\r') return false;
            if (ptr[pos] != ' ') break;
            ++pos;
        }

        if (pos == entry_end) return false;

        key_start = pos;

        // Last nonwhite or = is end of name
        while (pos != entry_end)
        {
            if (ptr[pos] == '\n' || ptr[pos] == '\r' || ptr[pos] == ' ' || ptr[pos] == '=') break;
            ++pos;
        }

        if (pos == entry_end) return false;

        key_end = pos;
        val_start = pos;
    }
    else
    {
        key_start = key_end = -1;
        val_start = dre_end;
    }

    // Find the start of the value
    if (quoted)
    {
        ssize_t pos = val_start;
        // First thing after '="' is start of value
        while (pos != entry_end)
        {
            if (ptr[pos] == '"') break;
            if (ptr[pos] != '=' && ptr[pos] != ' ') return false;
            ++pos;
        }

        if (pos == entry_end) return false;
        val_start = pos + 1;
    }
    else
    {
        ssize_t pos = val_start;
        // First nonwhite is start of value
        while (pos != entry_end)
        {
            if (ptr[pos] != '\n' && ptr[pos] != '\r' && ptr[pos] != ' ') break;
            ++pos;
        }

        if (pos == entry_end) return false;
        val_start = pos;
    }

    // Find the end of the value
    if (!multiline)
    {
        ssize_t pos = val_start;
        if (quoted)
        {
            // First quote is end of value
            while (pos != entry_end)
            {
                if (ptr[pos] == '"') break;
                ++pos;
            }
            if (pos == entry_end) return false;
        }
        else
        {
            // First quote is end of value
            while (pos != entry_end)
            {
                if (ptr[pos] == '\n' || ptr[pos] == '\r') break;
                ++pos;
            }
            // Whitespace trim
            while (pos != val_start)
            {
                if (ptr[pos-1] != ' ') break; // Exclusive
                --pos;
            }
        }
        val_end = pos;
    }
    else
    {
        ssize_t pos = entry_end;
        if (quoted)
        {
            // First quote is end of value
            while (pos != val_start)
            {
                --pos;
                if (ptr[pos] == '"') break; // Exclusive
            }
        }
        else
        {
            // CB: We do not count the last \n before #DRE
            if (pos != val_start && ptr[pos-1] != '\n')
            {
                --pos;
            }
            if (pos != val_start && ptr[pos-1] != '\r')
            {
                --pos;
            }
        }
        val_end = pos;
    }

    return true;
}

void FlexTableIDXParser::doneWithDRE()
{
    cr.seek(entry_end);
}

bool FlexTableIDXParser::writeOutField(size_t ks, size_t ke, size_t vs, size_t ve)
{
    size_t kl = ke - ks;
    size_t vl = ve - vs;
    char *dptr = (char*)cr.getDataPtr();

    char normalizedKey[256];
    if (kl >= sizeof(normalizedKey)) return false;
    normalize_key(dptr+ks, kl, normalizedKey);

    if (is_flextable)
    {
// *(int *)(0) = 0; // "Breakpoint"
        map_writer->append(getServerInterface(), normalizedKey, kl, false, dptr+vs, vl);
    }

    ImmutableStringPtr keyString(normalizedKey, kl);
    ColLkup::iterator it = real_col_lookup.find(keyString);

    if (it != real_col_lookup.end())
    {
        // Populate real column, with parsing
        int colNum = it->second;

        // Empty colums are null. CB TODO: Is this true?
        if (vl==0)
        {
            writer->setNull(colNum);
        }
        else
        {
            const Vertica::VerticaType &dt = colInfo.getColumnType(colNum);
            if (dt.getTypeOid() == TimestampTzOID)
            {
                // If all digits, this could be an epoch (seconds since 1970)
                char tv[32];
                size_t vp;
                for (vp=vs; vp < ve && (vp-vs)<31; ++vp)
                {
                    if (dptr[vp] < '0' || dptr[vp] > '9') break;
                    tv[vp-vs] = dptr[vp];
                }
                if (vp == ve)
                {
                    tv[vp-vs] = 0;
                    long long ll = atoll(tv);
                    Vertica::TimestampTz val = (ll - 946684800LL) * 1000000;
                    writer->setTimestampTz(colNum, val);
                    return true;
                }
            }

            return parseStringToType(dptr+vs, vl, colNum, colInfo.getColumnType(colNum), writer, sp);
        }

        // ??? usedCols.insert(it->second);
    }

    return true;
}

void FlexTableIDXParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, ((const char*)cr.getDataPtr())+dre_start, dre_end-dre_start,
                      std::string("#DRE"));
    crej.reject(rr);
}

#define TAG_IS(x) (((dre_end-dre_start) == ssize_t(x##_len)) && !strncmp(x, ((const char*)cr.getDataPtr())+dre_start, x##_len))

void FlexTableIDXParser::startNewRecord()
{
    if (isProcessingRecord) return;
    isProcessingRecord = true;

    // Make sure our output column is clean (especially in the case of previously-rejected rows)
    for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
        writer->setNull(realColNum);
    }

    if (map_writer) map_writer->clear();
}

void FlexTableIDXParser::endRecord()
{
    if (!isProcessingRecord) return;
    isProcessingRecord = false;

    if (map_col_index != -1) {
        VMapPairReader map_reader(*map_writer);

        size_t total_offset=0;
        VMapBlockWriter::convert_vmap(getServerInterface(), map_reader,
                                      map_data_buf, map_data_buf_len, total_offset);
        VString& map_data_col = writer->getStringRef(map_col_index);
        map_data_col.copy(map_data_buf, total_offset);
    }

    writer->next();
    recordsAcceptedInBatch++;
}

void FlexTableIDXParser::run() {
    const char *DREENDDOC =  "#DREENDDOC";  size_t DREENDDOC_len=strlen(DREENDDOC);
    const char *DREENDDATA = "#DREENDDATA"; size_t DREENDDATA_len=strlen(DREENDDATA);

    const char *DRETITLE = "#DRETITLE"; size_t DRETITLE_len=strlen(DRETITLE);
    const char *DREDATE = "#DREDATE"; size_t DREDATE_len=strlen(DREDATE);
    const char *DREREFERENCE = "#DREREFERENCE"; size_t DREREFERENCE_len=strlen(DREREFERENCE);
    const char *DREDBNAME = "#DREDBNAME"; size_t DREDBNAME_len=strlen(DREDBNAME);
    const char *DREFIELD = "#DREFIELD"; size_t DREFIELD_len=strlen(DREFIELD);
    const char *DRECONTENT = "#DRECONTENT"; size_t DRECONTENT_len=strlen(DRECONTENT);

    const char *DRESECTION = "#DRESECTION"; size_t DRESECTION_len=strlen(DRESECTION);
    const char *DRETYPE = "#DRETYPE"; size_t DRETYPE_len=strlen(DRETYPE);

    const char *DREDELETEREF = "#DREDELETEREF"; size_t DREDELETEREF_len=strlen(DREDELETEREF);
    const char *DREDELETEDOC = "#DREDELETEDOC"; size_t DREDELETEDOC_len=strlen(DREDELETEDOC);

    while (!cr.isEof())
    {
        if (!fetchNextDRETag()) break;

//getServerInterface().log("Char %c / %zu", ((const char *)cr.getDataPtr())[4], dre_end-dre_start);

        if (TAG_IS(DREENDDOC))
        {
            // This is the end of a document; on to the next one

//getServerInterface().log("Case 1");

            skipThisDRE(false);
            endRecord();
        }
        else if (TAG_IS(DREENDDATA))
        {
//getServerInterface().log("Case 2");
            // This is an EOF marker 
            // One wonders if we should trust it and stop, or just keep looking
            //  in case there's more data.  Because it could be accidentally
            //  inserted?
            break;
        }
        else if (TAG_IS(DRETYPE) ||
                 TAG_IS(DRESECTION) ||
                 TAG_IS(DREDELETEREF) ||
                 TAG_IS(DREDELETEDOC))
        {
//getServerInterface().log("Case 3");
            // We have heard tell of these
            //  but have no use for them now
            skipThisDRE(false);
        }
        else if (TAG_IS(DREFIELD))
        {
//getServerInterface().log("Case 4");
            // This is actually interesting
            startNewRecord();
            fetchThisDRE(true);
            parseThisDRE(true/*has field name*/, true/*quoted*/, true/*+multiline*/);
            writeOutField(key_start, key_end, val_start, val_end);
            doneWithDRE();
        }
        else if (TAG_IS(DRECONTENT))
        {
//getServerInterface().log("Case 5");
            // This is actually interesting,
            //  but it could be big... so we may want to skip it.
            if (storeContent)
            {
                startNewRecord();
                fetchThisDRE(false);
                parseThisDRE(false/*has field name*/, false/*quoted*/, true/*+multiline*/);
                writeOutField(dre_start+1, dre_end, val_start, val_end);
                doneWithDRE();
            }
            else
            {
                skipThisDRE(false);
            }
        }
        else if (TAG_IS(DREDATE) ||
                 TAG_IS(DREDBNAME) ||
                 TAG_IS(DREREFERENCE))
        {
//getServerInterface().log("Case 6");
            // This is a single-line value, and whitespace is to be trimmed
            startNewRecord();
            fetchThisDRE(false);
            parseThisDRE(false/*has field name*/, false/*quoted*/, false/*+multiline*/);
            writeOutField(dre_start+1, dre_end, val_start, val_end);
            doneWithDRE();
        }
        else if (TAG_IS(DRETITLE))
        {
//getServerInterface().log("Case 7");
            startNewRecord();
            // This may be multi-line up to #DRE
            fetchThisDRE(false);
            parseThisDRE(false/*has field name*/, false/*quoted*/, true/*+multiline*/);
            writeOutField(dre_start+1, dre_end, val_start, val_end);
            doneWithDRE();
        }
        else
        {
//getServerInterface().log("Case 8");
            // This is something unexpected
            //  For now let's just move on
            //  CB TODO: Could be DRESTORECONTENT or other directives
            skipThisDRE(false);
        }
    }

    endRecord();
}

void FlexTableIDXParser::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType)
{
    colInfo = returnType;

    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        const std::string &str = colInfo.getColumnName(col);
        // CB TODO: Use srvInterface
        char* normalizedKey = new char[str.size() + 1];
        normalize_key(str.c_str(), str.size(), normalizedKey);
        real_col_lookup[ImmutableStringPtr(normalizedKey, str.size())] = col;
    }

    // Find the flextable __raw__ column
    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        if ((0 == strcmp(colInfo.getColumnName(col).c_str(), RAW_COLUMN_NAME)) && colInfo.getColumnType(col).isStringType()) {
            map_col_index = col;
            map_data_buf_len = colInfo.getColumnType(col).getStringLength();
            map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
            break;
        }
    }

    if (map_data_buf_len > 0) {
        pair_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
        is_flextable = true;
        // CB TODO: Use srvInterface
        map_writer = new VMapPairWriter(pair_buf, map_data_buf_len);
    }
}

/// END class FlexTableIDXParser


/// BEGIN class FIDXParserFactory
void FIDXParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt)
{
    // Nothing to do here
}

UDParser* FIDXParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    ParamReader args(srvInterface.getParamReader());

    vbool store_content = vbool_true;
    if (args.containsParameter("store_content")) {
        store_content = args.getBoolRef("store_content");
    }

    return vt_createFuncObj(
        srvInterface.allocator,
        FlexTableIDXParser,
        store_content == vbool_true
    );
}

void FIDXParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FIDXParserFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes)
{
    parameterTypes.addBool("store_content");
}
/// END class FIDXParserFactory



// Register the parser factory
RegisterFactory(FIDXParserFactory);


} /// END namespace flextable
