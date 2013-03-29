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
            var dateField = "INVOICE_DATE",
                baseUrl = '<c:url value="/" />';

            UI.initWait();
            UI.initialize(SEARCH.ui.coreName, dateField, baseUrl, UI.returnKey);

        })();
    </script>
</layout:main>
