<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/tipsy.css"  />" />
    <link rel="stylesheet" href="<c:url value="/static/css/jquery/jquery-ui.css" />" type="text/css" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/plot.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/d3/d3.v3.js"  />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery-1.9.1.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery-ui.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery.tipsy.js" />"></script>

    <script type="text/javascript">

    (function() {
        var Dom  = YAHOO.util.Dom,        Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,         Event = YAHOO.util.Event,
            Slider = YAHOO.widget.Slider;

        var baseUrl = '<c:url value="/" />',
            plotUrl = baseUrl + 'search/plot',

            graphElId            = "graph",                graphOptionsElId     = "graph_options",
            seriesOptionsElId    = "graph_options_series_options",
            redrawBySeriesElId   = "redraw_with_series",   redrawSinglePlotElId = "redraw_single_plot",
            redrawElId           = "redraw",               contentEl            = Dom.get(UI.CONTENT_EL_NAME),
            xSliderElId          = "x_slider",             ySliderElId          = "y_slider",
            xMinSliderElId       = "x_min_slider_val",     yMinSliderElId       = "y_min_slider_val",
            xMaxSliderElId       = "x_max_slider_val",     yMaxSliderElId       = "y_max_slider_val",
            xSliderTextElId      = "x_slider_text",        ySliderTextElId      = "y_slider_text",
            xSliderSetMinValElId = "x_slider_set_min_val", ySliderSetMinValElId = "y_slider_set_min_val",
            xSliderSetMaxValElId = "x_slider_set_max_val", ySliderSetMaxValElId = "y_slider_set_max_val",
            xSliderSetMinElId    = "x_slider_set_min",     ySliderSetMinElId    = "y_slider_set_min",
            xSliderSetMaxElId    = "x_slider_set_max",     ySliderSetMaxElId    = "y_slider_set_max",
            xAxisName            = "x",                    yAxisName            = "y",
            seriesFieldName      = "series1",
            nFoundDivElName      = "n_found",              nNotNullDivElName    = "n_not_null",
            nFoundCtrDivElName   = "n_found_container",    sliderTxtLblCSSClass = "slider_text_label",
            sliderSetValsButtonsCSSClass = "slider_set_val_buttons",

            maxWidth             = 1000,                   maxHeight            = 600,
            marginTop            = 20,                     marginRight          = 150,
            marginBottom         = 100,                    marginLeft           = 100,
            width                = maxWidth - marginLeft - marginRight,
            height               = maxHeight - marginTop - marginBottom;

        var requestParamFields   = ["query", "fq", "core", "numfound", "xaxis", "yaxis", "series"],
            requestParams        = UI.util.getRequestParameters(),
            hasSeries            = (requestParams["series"] != undefined);

        var minX        = 0,            maxX               = maxWidth,
            minY        = 0,            maxY               = maxHeight,
            seriesData  = [],           allData            = [],
            xAxisIsDate = false,        yAxisIsDate        = false,
            slidePoints = false,        currPlotIsSeries   = false,
            svgEls      = {},           plotData           = {};

        var parseDate         = d3.time.format("%d-%b-%y").parse,
            parseFullYearDate = d3.time.format("%b-%d-%Y").parse;

        buildHtml();
        UI.initWait();

        Connect.asyncRequest('GET', plotUrl + UI.util.constructRequestString(requestParams), {
            success: function(o) {
                UI.hideWait();

                setGraphVariablesFromPlotData(Json.parse(o.responseText));
                drawGraph();
                initNFoundDivs();
            },
            failure: function(o) {
            }
        });

        Event.addListener(redrawBySeriesElId,   "click", function(o) { Dom.get(redrawSinglePlotElId).checked = false; });
        Event.addListener(redrawSinglePlotElId, "click", function(o) { Dom.get(redrawBySeriesElId).checked = false; });
        Event.addListener(redrawElId, "click", function(o) {
            currPlotIsSeries = hasSeries && Dom.get(redrawBySeriesElId).checked;
            drawGraph();
        });

        Event.addListener(xSliderSetMinElId, "click", function(o) { setMinMaxRangeValues(true, true); });
        Event.addListener(xSliderSetMaxElId, "click", function(o) { setMinMaxRangeValues(false, true); });
        Event.addListener(ySliderSetMinElId, "click", function(o) { setMinMaxRangeValues(true, false); });
        Event.addListener(ySliderSetMaxElId, "click", function(o) { setMinMaxRangeValues(false, false); });

        function adjust(range, isMin, isDate) {
            var min = UI.util.returnIfDefined(0.0, range['min']),
                max = UI.util.returnIfDefined(0.0, range['max']),
                val = isMin ? min : max;
            if (isDate) {
                max = parseDate(max).getTime();
                min = parseDate(min).getTime();
                val = parseDate(val).getTime();
            }

            var margin = (isMin ? -1 : 1)*(max - min)*.05;
            return isDate ? new Date(val + margin) : Math.floor(val + margin);
        }

        function setGraphVariablesFromPlotData(responseJSON) {
            plotData        = responseJSON;
            xAxisName       = plotData['xAxis'];
            yAxisName       = plotData['yAxis'];
            seriesFieldName = plotData['series'];
            xAxisIsDate     = plotData['xAxisIsDate'];
            yAxisIsDate     = plotData['yAxisIsDate'];
            minX            = adjust(plotData['xRange'], true, xAxisIsDate);
            maxX            = adjust(plotData['xRange'], false, xAxisIsDate);
            minY            = adjust(plotData['yRange'], true, yAxisIsDate);
            maxY            = adjust(plotData['yRange'], false, yAxisIsDate);
            seriesData      = plotData['data']['series'];
            allData         = plotData['data']['all'];

            slidePoints     = allData['series1'].length < 8000;
            currPlotIsSeries = seriesData != undefined && Object.keys(seriesData).length > 0;
        }

        function getLongestStringLength(names) {
            var l = 0;
            for(var i = 0; i < names.length; i++) {
                l = Math.max(names[i].length, l);
            }
            return l;
        }

        function getPoints(data) {
            var pts = UI.util.clone(data);

            if (xAxisIsDate || yAxisIsDate) {
                pts.forEach(function(d) {
                    if (xAxisIsDate && !(d.x instanceof Date)) d.x = parseDate(d.x);
                    if (yAxisIsDate && !(d.y instanceof Date)) d.y = parseDate(d.y);
                });
            }
            return pts;
        }

        function getPointOpacity(d) {
            var px = xAxisIsDate ? new Date(d.x).getTime() : d.x, py = d.y,
                yRange = $('#'+ ySliderElId).slider("values"), by = yRange[0], ey = yRange[1],
                xRange = $('#'+ xSliderElId).slider("values"),
                bx = xAxisIsDate ? new Date(xRange[0]).getTime() : xRange[0],
                ex = xAxisIsDate ? new Date(xRange[1]).getTime() : xRange[1];

            return (px > bx && px < ex && py > by && py < ey) ? 1 : 0;
        }

        function drawGraph() {
            removeSvg();

            updateSliderHTMLElements(true,  minX, maxX);
            updateSliderHTMLElements(false, minY, maxY);

            var data = currPlotIsSeries ? UI.util.clone(seriesData) : UI.util.clone(allData);
            var seriesNames = Object.keys(data);

            var x = (xAxisIsDate ? d3.time.scale() : d3.scale.linear()).domain([minX, maxX]).range([0, width]);
            var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);

            var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(10);
            if (xAxisIsDate) {
                xAxis.tickFormat(d3.time.format("%b-%d-%Y"));
            }
            var yAxis = d3.svg.axis().scale(y).orient("left").ticks(10);

            var svg = d3.select(Dom.get(graphElId)).append("svg")
                    .attr("width",  maxWidth + (UI.util.lengthOfLongestStringInArray(seriesNames) * 8))
                    .attr("height", maxHeight)
                    .append("g")
                    .attr("transform", "translate(" + marginLeft + "," + marginTop + ")");

            svg.append("g")
                    .attr("class", "x axis")
                    .attr("transform", "translate(0," + height + ")")
                    .call(xAxis)
                    .selectAll("text")
                        .style("text-anchor", "end")
                        .attr("dx", "-.8em")
                        .attr("dy", ".15em")
                        .attr("transform", function(d) { return "rotate(-65)" });

            svg.append("g")
                    .attr("class", "y axis")
                    .call(yAxis)
                    .append("text")
                    .attr("transform", "rotate(-90)")
                    .attr("y", 6)
                    .attr("dy", ".71em")
                    .style("text-anchor", "end")
                    .text(yAxisName);

            svg.append("text")
                    .attr("text-anchor", "middle")
                    .attr("transform", "translate(" + (width - 20 - xAxisName.length*3) + "," + (height - 10) + ")")
                    .text(xAxisName);

            svg.append("g")
                    .attr("class", "title")
                    .attr('transform', 'translate(40,-10)')
                    .append("text")
                        .text(xAxisName + " vs. " + yAxisName + (currPlotIsSeries ? ", separated by " + seriesFieldName : ""))
                        .style("fill", "black");

            var clip = svg.append("defs").append("svg:clipPath")
                    .attr("id", "clip")
                    .append("svg:rect")
                        .attr("id", "clip-rect")
                        .attr("x", "0")
                        .attr("y", "0")
                        .attr("width", width)
                        .attr("height", height);

            var line = d3.svg.line()
                             .x(function(d) { return x(d.x) })
                             .y(function(d) { return y(d.y) });

            for(var i = 0; i < seriesNames.length;  i++) {
                var seriesName = seriesNames[i],
                    lineColor  = makeColorGradient(i),
                    pts        = getPoints(data[seriesName]);

                var path = svg.append("svg:path")
                                    .attr("stroke", lineColor)
                                    .attr("fill-opacity", 0.0)
                                    .attr("class", "path")
                                    .attr("clip-path", "url(#clip)")
                                    .attr("id", "path" + i)
                                    .attr("d", line(pts));

                var circles = svg.selectAll("circle" + i)
                        .data(pts)
                        .enter()
                        .append("circle")
                            .attr("fill", lineColor)
                            .attr("cx", function(d) { return x(d.x); })
                            .attr("cy", function(d) { return y(d.y); })
                            .attr("id", "circle" + i)
                            .attr("r", function(d) { return (d.count > 1) ? 5 : 3 });

               $('svg circle').tipsy({
                    gravity: 'w',
                    html: true,
                    title: function() {
                        var d = this.__data__;
                        var x = xAxisIsDate ? UI.MONTHS[d.x.getMonth()] + "-" + d.x.getDate() + "-" + d.x.getFullYear() : d.x,
                            y = d.y;

                        var s = "Count: " + d.count + "<br>" + xAxisName + ": " + x + "<br>" + yAxisName + ": " + y;
                        if (currPlotIsSeries) s = "Series: " + seriesNames[parseInt(this.id.match(/circle(\d)/)[1])] + "<br>" + s;
                        return s;
                    }
                });

                svgEls[seriesName] = circles[0].concat(path[0]);
            }

            function zoom(isX, slidePoints, begin, end) {
                var fn           = isX ? x : y,
                    axis         = isX ? xAxis : yAxis,
                    axisSelector = isX ? ".x.axis" : ".y.axis",
                    isDate       = isX ? xAxisIsDate : yAxisIsDate,
                    minValue     = isDate ? new Date(begin) : begin,
                    maxValue     = isDate ? new Date(end) : end;

                fn.domain([minValue, maxValue]);

                updateSliderHTMLElements(isX, minValue, maxValue);

                var t = svg.transition().duration(0);
                var val, nSteps = axis.ticks()[0], size = end - begin, step = size/nSteps;
                var ticks = [];
                for (var i = 0; i <= nSteps; i++) {
                    val = isDate ? new Date(begin + step*i) : Math.floor(begin + step*i);
                    ticks.push(val);
                }

                axis.tickValues(ticks);
                var a = t.select(axisSelector).call(axis);

                if (isX) {
                    a.selectAll("text")
                            .style("text-anchor", "end")
                            .attr("dx", "-.8em").attr("dy", ".15em").attr("transform", function(d) { return "rotate(-65)" });
                }

                if (slidePoints) {
                    for(i = 0; i < seriesNames.length;  i++) {
                        var points = getPoints(data[seriesNames[i]]);
                        t.select('#path' + i).attr('d', line(points));
                        t.selectAll('#circle' + i)
                                .attr("opacity", function(d) { return getPointOpacity(d); })
                                .attr("cx", function(d) { return x(d.x); })
                                .attr("cy", function(d) { return y(d.y); })
                    }
                }
            }

            $(function() {
                var min = xAxisIsDate ? minX.getTime() : minX,
                    max = xAxisIsDate ? maxX.getTime() : maxX;

                 $("#" + xSliderElId).slider({
                    range : true, min : min, max : max, values : [min, max],
                    slide  : function(event, ui) { zoom(true, slidePoints, ui.values[0], ui.values[1]); },
                    stop   : function(event, ui) { zoom(true, true, ui.values[0], ui.values[1]); },
                    change : function(event, ui) { zoom(true, true, ui.values[0], ui.values[1]); }
                });

                $("#" + ySliderElId).slider({
                    range: true, min: minY, max: maxY, values: [minY, maxY],
                    slide  : function(event, ui) { zoom(false, slidePoints, ui.values[0], ui.values[1]); },
                    stop   : function(event, ui) { zoom(false, true, ui.values[0], ui.values[1]); },
                    change : function(event, ui) { zoom(false, true, ui.values[0], ui.values[1]); }
                });
            });

            addLegend(svg, seriesNames);
        }

        function setMinMaxRangeValues(isMin, isX) {
            var sliderValElId = isX ? (isMin ? xSliderSetMinValElId : xSliderSetMaxValElId) :
                                      (isMin ? ySliderSetMinValElId : ySliderSetMaxValElId),
                sliderElId    = isX ? xSliderElId : ySliderElId,
                isDate        = isX ? xAxisIsDate : yAxisIsDate;

            var parsed, val = Dom.get(sliderValElId).value;
            if (isDate) {
                parsed = parseDate(val);
                if (parsed == null) parsed = parseFullYearDate(val);
                if (parsed == null) {
                    alert("Please give date in format MMM-dd-yyyy");
                    return;
                }
                parsed = parsed.getTime();
            } else {
                parsed = parseFloat(val);
                if (isNaN(parsed)) {
                    alert("Invalid input " + val);
                    return;
                }
            }

            var range  = $('#'+ sliderElId).slider("values"),
                newMin =  isMin && parsed < range[1] ? parsed : range[0],
                newMax = !isMin && parsed > range[0] ? parsed : range[1];

            $('#'+ sliderElId).slider("values", [newMin, newMax]);
        }

        function addTitle(svg) {
            var legendX     = width - 65,
                    legendTextX = width - 52,
                    rectW       = 10, rectH = 10,
                    h           = 20,
                    grayColor   = "#808080";


        }

        function addLegend(svg, seriesNames) {
            var legendX     = width - 65,
                legendTextX = width - 52,
                rectW       = 10, rectH = 10,
                h           = 20,
                grayColor   = "#808080";

            var legend = svg.append("g")
                    .attr("class", "legend")
                    .attr('transform', 'translate(80,0)');

            legend.selectAll('rect')
                    .data(seriesNames)
                    .enter()
                    .append("rect")
                        .attr("x", legendX)
                        .attr("y", function(d, i){ return i*h + 1.5*h;})
                        .attr("width",  rectW)
                        .attr("height", rectH)
                        .attr("id", function(d, i) { return "rect_" + i; })
                        .style("fill", function(d, i) { return makeColorGradient(i); })
                        .style("stroke", "black");

            legend.selectAll('text')
                    .data(seriesNames)
                    .enter()
                    .append("text")
                    .attr("x", legendTextX)
                    .attr("y", function(d, i){ return i*h + 2*h; })
                    .text(function(d) { return d; })
                    .style("fill", function(d, i) { return makeColorGradient(i); });

            legend.append("text")
                    .attr("x", legendX)
                    .attr("y", -(h/2) + 1)
                    .text("Click box to show/hide")
                    .style("fill", "black");

            legend.append("rect")
                    .attr("x", legendX)
                    .attr("y", 0)
                    .attr("width",  rectW)
                    .attr("height", rectH)
                    .attr("id", "rect_show_hide")
                    .style("fill", grayColor)
                    .style("stroke", "black");

            legend.append("text")
                    .attr("x", legendTextX)
                    .attr("y", h/2)
                    .attr("id", "text_show_hide")
                    .text("Show/hide all")
                    .style("fill", "black");

            Dom.get("rect_show_hide").onclick = function(o) {
                var visible = Dom.getStyle(this, "fill") == grayColor;
                Dom.setStyle(this, "fill", visible ? "white" : grayColor);
                for(var k = 0; k < seriesNames.length; k++) {
                    changeSeriesVisibility(k, svgEls[seriesNames[k]], visible);
                }
            };

            for(var i = 0; i < seriesNames.length; i++) {
                Dom.get("rect_" + i).onclick = function(o) {
                    var index = parseInt(this.id.match(/rect_(.*)/)[1]);
                    var visible = Dom.getStyle(svgEls[seriesNames[index]][0], "visibility") == "visible";
                    changeSeriesVisibility(index, svgEls[seriesNames[index]], visible);
                }
            }
        }

        function changeSeriesVisibility(index, els, visible) {
            var rect = Dom.get("rect_" + index);
            Dom.setStyle(rect, "fill", visible ? "white" : makeColorGradient(index));
            for(var j = 0; j < els.length; j++) {
                Dom.setStyle(els[j], "visibility", visible ? "hidden" : "visible");
            }
        }

        function removeSvg() {
            var svg = document.getElementsByTagName("svg")[0];
            if (svg != undefined) {
                svg.parentNode.removeChild(svg);
            }
        }

        function byte2Hex(n) {
            var nybHexString = "0123456789ABCDEF";
            return String(nybHexString.substr((n >> 4) & 0x0F,1)) + nybHexString.substr(n & 0x0F,1);
        }
        function RGB2Color(r,g,b) { return '#' + byte2Hex(r) + byte2Hex(g) + byte2Hex(b); }

        function makeColorGradient(n) {
            var f1 = 1.5, f2 = .5, f3 = .5, ph1 = 0, ph2 = 5, ph3 = 4, c = 128, w = 100;
            return RGB2Color(Math.sin(f1*n + ph1)*w + c, Math.sin(f2*n + ph2)*w + c, Math.sin(f3*n + ph3)*w + c);
        }

        function buildSliderHtml(isX) {
            var sliderElId    = isX ? xSliderElId : ySliderElId,                   sliderTextElId = isX ? xSliderTextElId : ySliderTextElId,
                minSliderElId = isX ? xMinSliderElId : yMinSliderElId,             maxSliderElId  = isX ? xMaxSliderElId : yMaxSliderElId,
                setMinElId    = isX ? xSliderSetMinElId : ySliderSetMinElId,       setMaxElId     = isX ? xSliderSetMaxElId : ySliderSetMaxElId,
                setMinValElId = isX ? xSliderSetMinValElId : ySliderSetMinValElId, setMaxValElId  = isX ? xSliderSetMaxValElId : ySliderSetMaxValElId,
                axis          = isX ? "X" : "Y";

            var div = UI.addDomElementChild('div', Dom.get(graphOptionsElId), {}, { "margin-top" : "20px"});
            UI.addDomElementChild('div', div, { id : sliderElId });

            var textDiv = UI.addDomElementChild('div', div, { id : sliderTextElId });
            UI.addDomElementChild('label', textDiv, { innerHTML: axis + " min: "}, { "class" : sliderTxtLblCSSClass });
            UI.addDomElementChild('div', textDiv, { id: minSliderElId });
            UI.addDomElementChild('label', textDiv, { innerHTML: axis + " max: "}, { "class" : sliderTxtLblCSSClass });
            UI.addDomElementChild('div', textDiv, { id: maxSliderElId});
            UI.addClearBothDiv(div);

            var setMinMaxDiv = UI.addDomElementChild('div', div, {}, { "margin-bottom" : "30px"});
            UI.addDomElementChild('input', setMinMaxDiv, { id : setMinElId, type : "button", value: "Set" }, { "class" : sliderSetValsButtonsCSSClass});
            UI.addDomElementChild('input', setMinMaxDiv, { id : setMinValElId, type : "text" });
            UI.addDomElementChild('input', setMinMaxDiv, { id : setMaxElId, type : "button", value: "Set" }, { "class" : sliderSetValsButtonsCSSClass});
            UI.addDomElementChild('input', setMinMaxDiv, { id : setMaxValElId, type : "text"});

            UI.addClearBothDiv(div);
        }

        function buildNFoundDiv(graphOptionsEl, labelElId, labelText) {
            var div = UI.addDomElementChild('div', graphOptionsEl, { id : nFoundCtrDivElName });
            UI.addDomElementChild('label', div, { innerHTML: labelText });
            UI.addDomElementChild('span',  div, { id : labelElId });
            UI.addClearBothDiv(div);
        }

        function buildHtml() {
            Dom.setStyle(contentEl, "width", "2000px");

            UI.addDomElementChild('div', contentEl, { id : graphElId });

            var graphOptionsEl = UI.addDomElementChild('div', contentEl, { id : graphOptionsElId });
            buildNFoundDiv(graphOptionsEl, nFoundDivElName,   "Total number of records found");
            buildNFoundDiv(graphOptionsEl, nNotNullDivElName, "Total number of records with valid (X, Y) values");

            buildSliderHtml(true);
            buildSliderHtml(false);

            if (hasSeries) {
                var div = UI.addDomElementChild('div', graphOptionsEl, { id : seriesOptionsElId });
                UI.addDomElementChild('span',  div, { innerHTML: "Redraw" }, { "margin-left" : "0" });
                UI.addDomElementChild('input', div, { type : "checkbox", id : redrawBySeriesElId, checked : true });
                UI.addDomElementChild('span',  div, { innerHTML: "Series" });

                UI.addDomElementChild('input', div, { type : "checkbox", id : redrawSinglePlotElId });
                UI.addDomElementChild('span',  div, { innerHTML: "Single plot" });
                UI.addClearBothDiv(div);
            }

            UI.addDomElementChild('a', graphOptionsEl, { id : redrawElId, innerHTML : "Redraw" }, { "class" : "button small" } );
        }

        var convertDateSliderVal = function (date) {
            return UI.MONTHS[date.getMonth()] + "-" + date.getDate() + "-" + date.getFullYear();
        };

        var updateSliderHTMLElements = function (xSlider, newMin, newMax) {
            var isDate      = xSlider ? xAxisIsDate : yAxisIsDate,
                minSliderEl = xSlider ? Dom.get(xMinSliderElId) : Dom.get(yMinSliderElId),
                maxSliderEl = xSlider ? Dom.get(xMaxSliderElId) : Dom.get(yMaxSliderElId);

            minSliderEl.innerHTML = isDate ? convertDateSliderVal(newMin) : parseFloat(newMin).toFixed(2);
            maxSliderEl.innerHTML = isDate ? convertDateSliderVal(newMax) : parseFloat(newMax).toFixed(2);
        };

        function initNFoundDivs() {
            Dom.get(nFoundDivElName).innerHTML   = ": " + plotData['nFound'];
            Dom.get(nNotNullDivElName).innerHTML = ": " + plotData['nNotNull'];
        }

    })();
    </script>
</layout:main>
