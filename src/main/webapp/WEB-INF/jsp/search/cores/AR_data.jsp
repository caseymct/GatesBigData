<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ page pageEncoding="UTF-8" %>

<layout:main>
<link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

<script type="text/javascript" src="<c:url value="/static/js/search/dataTabview.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/search/search.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/search/queryBuilder.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/search/searchAutoComplete.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/search/datepick.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/search/facet.js" />"></script>

<script type="text/javascript" src="<c:url value="/static/js/search/export.js" />"></script>

<script type="text/javascript">
    (function() {

        var     dateField = "TRANSACTION_DATE",
                core = SEARCH.ui.coreName,
                coreUrlReqStr  = "core=" + core,
                fieldUrlReqStr = "field=" + dateField,
                searchBaseUrl = '<c:url value="/search/" />';

        var urls = {};
        urls[UI.SEARCH_BASE_URL_KEY]        = searchBaseUrl;
        urls[UI.DATE_PICKER_URL_KEY]        = '<c:url value="/core/field/daterange?" />' + coreUrlReqStr + "&" + fieldUrlReqStr;
        urls[UI.FACET_URL_KEY]              = searchBaseUrl + "solrfacets?" + coreUrlReqStr;
        urls[UI.SEARCH_URL_KEY]             = searchBaseUrl + "solrquery";
        urls[UI.SUGGEST_URL_KEY]            = searchBaseUrl + "suggest?" + coreUrlReqStr;
        urls[UI.QUERY_BUILDER_AC_URL_KEY]   = searchBaseUrl + "fields/all?" + coreUrlReqStr;
        urls[UI.JSP_EXPORT_URL_KEY]         = '<c:url value="/core/export/" />' + core;
        urls[UI.VIEW_DOC_URL_KEY]           = '<c:url value="/core/document/prizmview" />';
        urls[UI.LOADING_IMG_URL_KEY]        = '<c:url value="/static/images/loading.png" />';
        urls[UI.THUMBNAIL_URL_KEY]          = '<c:url value="/document/thumbnail/get" />';

        var columnDefs = [
            {key: 'INVENTORY_LOC_KEY', label: 'INVENTORY_LOC_KEY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
            {key: 'PRODUCT_KEY', label: 'PRODUCT_KEY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'BILLTO_CUSTOMER_KEY', label: 'BILLTO_CUSTOMER_KEY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'SHIPTO_CUSTOMER_KEY', label: 'SHIPTO_CUSTOMER_KEY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ORDER_HEADER_ID', label: 'ORDER_HEADER_ID', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ORDER_NUMBER', label: 'ORDER_NUMBER', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ORDER_CREATION_DATE', label: 'ORDER_CREATION_DATE', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'TRANSACTION_DATE', label: 'TRANSACTION_DATE', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'LINE_ACTUAL_SHIPMENT_DATE', label: 'LINE_ACTUAL_SHIPMENT_DATE', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'LOAD_DATE', label: 'LOAD_DATE', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'LIST_PRICE_ACTUAL', label: 'LIST_PRICE_ACTUAL', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'SELLING_PRICE_ACTUAL', label: 'SELLING_PRICE_ACTUAL', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'GROSS_SALES_ACTUAL', label: 'GROSS_SALES_ACTUAL', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'NET_SALES_ACTUAL', label: 'NET_SALES_ACTUAL', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'TOTAL_PRODUCT_COST', label: 'TOTAL_PRODUCT_COST', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ULTIMATE_DEST_CITY', label: 'ULTIMATE_DEST_CITY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ULTIMATE_DEST_STATE', label: 'ULTIMATE_DEST_STATE', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ULTIMATE_DEST_COUNTY', label: 'ULTIMATE_DEST_COUNTY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
			{key: 'ULTIMATE_DEST_COUNTRY', label: 'ULTIMATE_DEST_COUNTRY', sortable: true, formatter: SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth },
            {key: 'thumbnail', label:'thumbnail', hidden: true},
            {key: 'thumbnailType', label:'thumbnailType', hidden: true}
        ];

        var dataSourceFields = [
            {key: 'INVENTORY_LOC_KEY', parser: 'text'},
            {key: 'PRODUCT_KEY', parser: 'text'},
            {key: 'BILLTO_CUSTOMER_KEY', parser: 'text'},
            {key: 'SHIPTO_CUSTOMER_KEY', parser: 'text'},
            {key: 'ORDER_HEADER_ID', parser: 'text'},
            {key: 'ORDER_NUMBER', parser: 'text'},
            {key: 'ORDER_CREATION_DATE', parser: 'text'},
            {key: 'TRANSACTION_DATE', parser: 'text'},
            {key: 'LINE_ACTUAL_SHIPMENT_DATE', parser: 'text'},
            {key: 'LOAD_DATE', parser: 'text'},
            {key: 'LIST_PRICE_ACTUAL', parser: 'text'},
            {key: 'SELLING_PRICE_ACTUAL', parser: 'text'},
            {key: 'GROSS_SALES_ACTUAL', parser: 'text'},
            {key: 'NET_SALES_ACTUAL', parser: 'text'},
            {key: 'TOTAL_PRODUCT_COST', parser: 'text'},
            {key: 'ULTIMATE_DEST_CITY', parser: 'text'},
            {key: 'ULTIMATE_DEST_STATE', parser: 'text'},
            {key: 'ULTIMATE_DEST_COUNTY', parser: 'text'},
            {key: 'ULTIMATE_DEST_COUNTRY', parser: 'text'},
            {key: 'id', parser:'text'}
        ];

        UI.initWait();

        var dataTabViewVars = {};
        dataTabViewVars[UI.SELECTED_CORE_KEY] = core;
        dataTabViewVars[UI.URLS_KEY] = urls;
        dataTabViewVars[UI.DATE_FIELD_KEY] = dateField;
        dataTabViewVars[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY] = columnDefs;
        dataTabViewVars[UI.SEARCH.DATA_SOURCE_FIELDS_KEY] = dataSourceFields;
        dataTabViewVars[UI.CORE_NAMES_KEY] = ["NA_data", "AR_data"];
        dataTabViewVars[UI.TAB_DISPLAY_NAMES_KEY] = [ "AP Data", "AR Data"];
        dataTabViewVars[UI.DATA_TYPE_KEY] = UI.DATA_TYPE_STRUCTURED;
        DATA_TABVIEW.init(dataTabViewVars);

    })();
</script>
</layout:main>