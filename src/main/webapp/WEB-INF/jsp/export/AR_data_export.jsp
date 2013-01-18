<%@ page session="false" contentType="text/html" pageEncoding="ISO-8859-1" import="java.util.*,javax.portlet.*" %>

<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
<%@ taglib prefix="form" uri="http://www.springframework.org/tags/form" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/export.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/export/exportoptions.js" />"></script>

    <script type="text/javascript">
        (function() {

            var exportOptionsParams = {};
            exportOptionsParams[UI.URLS_KEY] = {};
            exportOptionsParams[UI.URLS_KEY][UI.EXPORT_URL_KEY]     = '<c:url value="/export" />';
            exportOptionsParams[UI.URLS_KEY][UI.FIELD_NAMES_URL_KEY]= '<c:url value="/core/fieldnames" />';

            EXPORTOPTIONS.init(exportOptionsParams);

        })();
    </script>
</layout:main>