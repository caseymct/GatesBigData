var SEARCHAUTOCOMPLETE = {};
SEARCHAUTOCOMPLETE.ui = {};
SEARCHAUTOCOMPLETE.util = {};

(function() {
    var XHRDataSource = YAHOO.util.XHRDataSource,    AutoComplete    = YAHOO.widget.AutoComplete,
        Event         = YAHOO.util.Event,            Dom             = YAHOO.util.Dom;

    var acInputElName     = "autocomplete_input",
        acContainerElName = "autocomplete_container",
        acTabElName       = "autocomplete_tab",
        acDivElName       = "autocomplete_div",
        acTabCSSClass     = "search_tab_style row",
        suggestUrl        = "",
        searchFn          = undefined,
        acKeyupFn         = undefined;

    SEARCHAUTOCOMPLETE.ui.init = function(vars) {
        suggestUrl  = vars.url;
        searchFn    = vars.searchFn;
        acKeyupFn   = vars.acKeyupFn;

        buildHTML(vars.tabListElName, vars.tabContentElName);

        var ds = new XHRDataSource(suggestUrl);
        ds.responseType = XHRDataSource.TYPE_JSON;
        ds.responseSchema = { resultsList : "suggestions" };

        var searchAC = new AutoComplete(acInputElName, acContainerElName, ds);
        searchAC.generateRequest = function() {
            return '&n=5&userinput=' + this.getInputEl().value.encodeForRequest();
        };
        searchAC.itemSelectEvent.subscribe(itemSelectHandler);
    };

    SEARCHAUTOCOMPLETE.ui.getAutocompleteInputElName = function() {
        return acInputElName;
    };

    function itemSelectHandler(s, args) {
        var inputEl = Dom.get(args[0].getInputEl());
        var sel = args[2][0].match("<b>(.*)</b> <i>(.*)</i>.*");
        inputEl.value = sel[2] + ":\"" + sel[1] + "\"";
        searchFn();
    }

    Event.onContentReady(acInputElName, function() {
        Event.addListener(acInputElName, "keyup", function(e) {
            acKeyupFn();
        });
    });

    function buildHTML(tabListElName, tabContentElName) {
        var tabList = Dom.get(tabListElName);
        var li = UI.addDomElementChild('li', tabList, {}, { class: "selected tab_selected" });
        UI.addDomElementChild('a', li, { href : "#tab1", innerHTML: "<em>Search</em>"});

        var tabNode = Dom.get(tabContentElName);
        var tabDiv = UI.addDomElementChild('div', tabNode, {id : acTabElName }, {class : acTabCSSClass});
        var div = UI.addDomElementChild('div', tabDiv, {id : acDivElName});
        UI.addDomElementChild('input', div, {id : acInputElName, type: "text"});
        UI.addDomElementChild('div', div, {id: acContainerElName});
    }

})();