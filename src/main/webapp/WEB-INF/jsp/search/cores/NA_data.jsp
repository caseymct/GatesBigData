<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ page pageEncoding="UTF-8" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/search/dataTabview.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/search.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/queryBuilder.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/datepick.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/facet.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/searchAutoComplete.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/export.js" />"></script>

    <script type="text/javascript">
    (function() {
        var Event = YAHOO.util.Event;

        var dateField = "InvoiceDate",
            core = SEARCH.ui.coreName;

        var coreUrlReqStr  = "core=" + core,
            fieldUrlReqStr = "field=" + dateField;

        var datePickerUrl     = '<c:url value="/core/field/daterange?" />' + coreUrlReqStr + "&" + fieldUrlReqStr,
            searchBaseUrl     = '<c:url value="/search/" />',
            facetUrl          = searchBaseUrl + "solrfacets?" + coreUrlReqStr,
            searchUrl         = searchBaseUrl + "solrquery",
            suggestUrl        = searchBaseUrl + "suggest?" + coreUrlReqStr,
            queryBuilderACUrl = searchBaseUrl + "fields/all?" + coreUrlReqStr,
            exportUrl         = '<c:url value="/core/export/" />' + core,
            viewDocUrl        = '<c:url value="/core/document/prizmview" />',
            loadingImgUrl     = '<c:url value="/static/images/loading.png" />',
            thumbnailUrl      = '<c:url value="/document/thumbnail/get" />';

        var urls = { datePickerUrl : datePickerUrl, searchBaseUrl : searchBaseUrl, facetUrl : facetUrl, suggestUrl : suggestUrl,
                     searchUrl : searchUrl, queryBuilderAutoCompleteUrl : queryBuilderACUrl, exportUrl : exportUrl, viewDocUrl : viewDocUrl,
                     loadingImgUrl : loadingImgUrl, thumbnailUrl : thumbnailUrl };

        var columnDefs = [
            {key:'PaidDate', label:'Paid Date', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.shortStringWidth },
            {key:'InvoiceDate', label:'Invoice Date', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.shortStringWidth },
            {key:'Account.AccountName', label:'Account Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'AccountCompanyCode', label:'Company Code', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'User.UserName', label:'User Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CompanySite.SiteName', label:'Site Name', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'Supplier.SupplierName', label:'Supplier Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenter.CostCenterName', label:'Cost Center Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenterId', label:'Cost Center ID', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'InvoiceId', label:'Invoice ID', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'thumbnail', label:'thumbnail', hidden: true},
            {key:'thumbnailType', label:'thumbnailType', hidden: true}
        ];

        var dataSourceFields = [
            {key:'Account.AccountName', parser:'text'},
            {key:'AccountCompanyCode', parser:'text'},
            {key:'User.UserName', parser:'text'},
            {key:'Amount', parser:'number'},
            {key:'CompanySite.SiteName', parser:'text'},
            {key:'CostCenter.CostCenterName', parser:'text'},
            {key:'Supplier.SupplierName', parser:'text'},
            {key:'CostCenterId', parser:'text'},
            {key:'InvoiceId', parser:'number'},
            {key:'PaidDate', parser:'text'},
            {key:'InvoiceDate', parser:'text'},
            {key:'url', parser:'text'},
            {key:'id', parser:'text'}
        ];

        UI.initWait();


        DATA_TABVIEW.init({
            selectedCore : core,
            urls : urls,
            dateField : dateField,
            selectDataColumnDefs : columnDefs,
            dataSourceFields : dataSourceFields,
            formatSearchResultFn : createSubObjects,
            coreNames: ["NA_data", "AR_data"]
        });

        function createSubObjects(response) {
            var i, j, ret, key, keys, doc, docs = response['docs'];

            for(i = 0; i < docs.length; i++) {
                doc = docs[i];
                ret = new Object();
                keys = Object.keys(doc);
                
                for(j = 0; j < keys.length; j++) {
                    key = keys[j];
                    var val = doc[key], idx = key.indexOf(".");
                    
                    if (idx >= 0) {
                        var splitKey = key.split("."), parent = splitKey[0], child = splitKey[1];
                        if (!ret.hasOwnProperty(parent)) {
                            ret[parent] = new Object();
                        }
                        ret[parent][child] = val;
                    } else {
                        ret[key] = val;
                    }
                }
                response.docs[i] = ret;
            }

            return response;
        }

    })();
    </script>
</layout:main>
