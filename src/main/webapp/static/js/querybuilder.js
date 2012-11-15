var QUERYBUILDER = {};
QUERYBUILDER.ui = {};
QUERYBUILDER.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,              ButtonGroup     = YAHOO.widget.ButtonGroup,
        LocalDataSource = YAHOO.util.LocalDataSource,      AutoComplete    = YAHOO.widget.AutoComplete,
        Event           = YAHOO.util.Event,                Json            = YAHOO.lang.JSON,
        Dom             = YAHOO.util.Dom;

    var buttonTextToDelimiterMap = { 'Some' : 'OR', 'All' : 'AND' };

    var genQuerySearchInputElName       = "general_query_search_input",
        fieldConstraintElName           = "general_query_field_constraint",
        fieldAutoCompleteContainerName  = "general_query_field_autocomplete_container",
        hasPhraseFieldElName            = "has_phrase",
        isExactFieldElName              = "is_exact",
        hasWordsFieldElName             = "has_words",
        doesNotHaveWordsFieldElName     = "does_not_have_words",
        hasWordsButtonGroupDiv          = "has_words_div",
        doesNotHaveWordsButtonGroupDiv  = "does_not_have_words_div",
        addToQueryElName                = "add_to_query",
        generalQueryTabElName           = "search_generalquery_tab",
        fieldsetRowCSSClass             = "fieldset_row";

    var constraintFieldNames = [fieldConstraintElName, hasPhraseFieldElName, isExactFieldElName, hasWordsFieldElName, doesNotHaveWordsFieldElName];

    function getButtonGroupValue(bg) {
        return buttonTextToDelimiterMap[UI.getButtonGroupCheckedButtonValue(bg)];
    }

    function clearConstraintFields() {
        for(var i = 0; i < constraintFieldNames.length; i++) {
            Dom.get(constraintFieldNames[i]).value = "";
        }
    }

    QUERYBUILDER.ui.populateFieldAutoCompleteUrl = "";
    QUERYBUILDER.ui.initPopulateFieldAutoCompleteUrl = function(url) {
        QUERYBUILDER.ui.populateFieldAutoCompleteUrl = url;
    };

    function adjustGeneralQueryByConstraint(hasWordsButtonGroup, doesNotHaveWordsButtonGroup) {
        var generalQueryInput = Dom.get(genQuerySearchInputElName);
        var currStr = generalQueryInput.value;
        if (currStr == "*:*") currStr = "";

        var field       = Dom.get(fieldConstraintElName).value,
            phrase      = Dom.get(hasPhraseFieldElName).value,
            exact       = Dom.get(isExactFieldElName).value,
            wordsArr    = Dom.get(hasWordsFieldElName).value.split(/[\s,]+/),
            notwordsArr = Dom.get(doesNotHaveWordsFieldElName).value.split(/[\s,]+/),
            words       = wordsArr.join(' ' + getButtonGroupValue(hasWordsButtonGroup) + ' '),
            notwords    = notwordsArr.join(' ' + getButtonGroupValue(doesNotHaveWordsButtonGroup) + ' '),
            newStr      = field + ':',
            match       = currStr.match(field + ':'),
            matchEnd    = currStr.match('(' + field + ':\\(.+\\))$'),
            matchMiddle = currStr.match('(' + field + ':\\(.+\\)) [AND|OR]+ \\w+[\\.\\w]*:');
        if (words == '' && notwords == '' && phrase == '' && exact == '') return;

        if (match == null && currStr.indexOf(':') > -1 && currStr.endsWith([' AND ', ' OR ']) == false) {
            currStr += ' AND ';
        }
        if (exact != "") {
            newStr += '"' + exact + '"';
        } else {
            var p = [];
            if (words !== '') p.push((wordsArr.length == 1) ? words : '(' + words + ')');
            if (notwords !== '') p.push('NOT ' + ((notwordsArr.length == 1) ? notwords : '(' + notwords + ')'));
            if (phrase !== '') {
                if (!(phrase.startsWith('"') && phrase.endsWith('"'))) phrase = '"' + phrase + '"';
                p.push(phrase);
            }
            newStr += '(' + p.join(' AND ') + ')';
        }

        if (match != null) {
            generalQueryInput.value = currStr.replace((matchMiddle != null) ? matchMiddle[1] : matchEnd[1], newStr);
        } else {
            generalQueryInput.value = currStr + newStr;
        }
        clearConstraintFields();
    }

    Event.onContentReady(generalQueryTabElName, function() {
        /* Query constraint buttongroup code */
        var doesNotHaveWordsButtonGroup = new ButtonGroup(doesNotHaveWordsButtonGroupDiv);
        doesNotHaveWordsButtonGroup.check(1);
        var hasWordsButtonGroup = new ButtonGroup(hasWordsButtonGroupDiv);
        hasWordsButtonGroup.check(1);

        /* Populate the autocomplete field */
        Connect.asyncRequest('GET', QUERYBUILDER.ui.populateFieldAutoCompleteUrl, {
            success: function(o) {
                var response = Json.parse(o.responseText);
                var fieldac = new AutoComplete(fieldConstraintElName, fieldAutoCompleteContainerName, new LocalDataSource(response));
                fieldac.typeAhead = true;
            }
        });

        Event.addListener(addToQueryElName, "click", function(e) {
            Event.stopEvent(e);
            adjustGeneralQueryByConstraint(hasWordsButtonGroup, doesNotHaveWordsButtonGroup);
        });
    });

    QUERYBUILDER.ui.buildQueryTabHTML = function(tabContentEl) {
        var tabNode = Dom.get(tabContentEl);
        var p = UI.addDomElementChild('div', tabNode, [ {key: "id", value: generalQueryTabElName } ],
                                                        [ {key: "class", value: "search_tab_style row"} ]);
        p.innerHTML = 
            "<div class='row' style='padding: 2px'>" + 
            "   <textarea id='" + genQuerySearchInputElName + "'>*:*</textarea>" +
            "</div>" +
            "<fieldset style='padding-top:0'>" +
            "   <legend class='search_legend'>Add a constraint: </legend>" +
            "   <div class='" + fieldsetRowCSSClass + "'>" +
            "       <label for='" + fieldConstraintElName + "' style='margin-top:10px'>Field name:</label>" +
            "       <div id='general_query_autocomplete'>" +
            "           <input id='" + fieldConstraintElName + "' type='text'>" +
            "           <div id='" + fieldAutoCompleteContainerName + "'></div>" +
            "       </div>"+
            "   </div>" +
            "   <div class='" + fieldsetRowCSSClass + "'>" +
            "       <label id='" + hasWordsFieldElName + "_label' for='" + hasWordsFieldElName + "'>Has words: </label>" +
            "       <div class= 'fieldset_row_button_div' id='" + hasWordsButtonGroupDiv + "'>" +
            "            <input id='" + hasWordsFieldElName + "_some' type='radio' name='" + hasWordsFieldElName + "_input' value='Some' >" +
            "            <input id='" + hasWordsFieldElName + "_all' type='radio' name='" + hasWordsFieldElName + "_input' value='All' >" +
            "       </div>" +
            "       <input id='" + hasWordsFieldElName + "'/>" +
            "   </div>" +
            "   <div class='" + fieldsetRowCSSClass + "'>" +
            "       <label id='" + doesNotHaveWordsFieldElName + "_label' for='" + doesNotHaveWordsFieldElName + "'>Does not have words: </label>" +
            "       <div class= 'fieldset_row_button_div' id='" + doesNotHaveWordsButtonGroupDiv + "'>" +
            "           <input id='" + doesNotHaveWordsFieldElName + "_some' type='radio' name='" + doesNotHaveWordsFieldElName + "_input' value='Some' >" +
            "           <input id='" + doesNotHaveWordsFieldElName + "_all' type='radio' name='" + doesNotHaveWordsFieldElName + "_input' value='All' >" +
            "       </div>" +
            "       <input id='" + doesNotHaveWordsFieldElName + "'/>" +
            "   </div>" +
            "   <div class='" + fieldsetRowCSSClass + "' style='padding-top:10px'>" +
            "       <label for='" + hasPhraseFieldElName + "'>Has phrase: </label>" +
            "       <input id='" + hasPhraseFieldElName + "'/>" +
            "   </div>" +
            "   <div class='" + fieldsetRowCSSClass + "' style='padding-top:10px'>" +
            "       <label for='" + isExactFieldElName + "'>Is exactly: </label>" +
            "       <input id='" + isExactFieldElName + "'/>" +
            "   </div>" +
            "   <div class='" + fieldsetRowCSSClass + "'>" +
            "       <a href='#' id='" + addToQueryElName + "'>Add</a>" +
            "   </div>" +
            "   <div class='clearboth'></div>" +
            "   </fieldset>" +
            "   <div class='clearboth'></div>" +
            "</div> ";
        }
})();
