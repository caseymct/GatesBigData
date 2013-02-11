function _setElementProperties(el, properties, fn) {
    if (properties != undefined && properties != null) {
        var keys = Object.keys(properties);
        for(var i = 0; i < keys.length; i++) {
            el[fn](keys[i], properties[keys[i]]);
        }
    }
    return el;
}

function returnIfDefined(t, defaultValue) {
    return (t == undefined) ? defaultValue : t;
}

function highlight(el) {
    el.attr(WordTree.STROKE_CSS_KEY, WordTree.NODE_HIGHLIGHT_BORDER_COLOR);
}

function unhighlight(el, color) {
    el.attr(WordTree.STROKE_CSS_KEY, color);
}

function unhighlightNode(tree) {
    var node = tree.nDatabaseNodes[tree.iSelectedNode];
    node.isSelected = false;
    unhighlight(node.raphaelElements[WordTree.BOUNDING_RECT_KEY], node.borderColor);
    tree.iSelectedNode = -1;
}

WordNode = function (conf) {
    this.id     = conf['id'];
	this.pid    = conf['pid'];
	this.dsc    = conf['dsc'];
	this.w      = conf['w'];
	this.h      = conf['h'];
	this.target = conf['target'];
    this.paper  = conf['paper'];
    this.size   = conf['size'];

	this.siblingIndex = 0;
	this.dbIndex = 0;
	
	this.XPosition = 0;
	this.YPosition = 0;
	this.prelim = 0;
	this.modifier = 0;
	this.leftNeighbor = null;
	this.rightNeighbor = null;
	this.nodeParent = null;	
	this.nodeChildren = [];
	
	this.isCollapsed = false;
	this.canCollapse = false;
	
	this.isSelected = false;

    this.color = 'black';
    this.borderColor = 'lightgray';
    this._initRaphaelElementsObject();
};

WordNode.prototype._initRaphaelElementsObject = function() {
    this.raphaelElements = {};
    this.raphaelElements[WordTree.COLLAPSE_IMG_KEY] = null;
    this.raphaelElements[WordTree.TRANS_IMG_KEY]    = null;
    this.raphaelElements[WordTree.BOUNDING_RECT]    = null;
    this.raphaelElements[WordTree.SVG_ELS_KEY]      = [];
    this.raphaelElements[WordTree.PATHS_KEY]        = [];
};

WordNode.prototype._getLevel = function () {
	return (this.nodeParent.id == -1) ? 0 : this.nodeParent._getLevel() + 1;
};

WordNode.prototype._isAncestorCollapsed = function () {
	if (this.nodeParent.isCollapsed) return true;
    if (this.nodeParent.id == -1) return false;

	return this.nodeParent._isAncestorCollapsed();
};

WordNode.prototype._setAncestorsExpanded = function () {
	if (this.nodeParent.id == -1) return;

	this.nodeParent.isCollapsed = false;
    return this.nodeParent._setAncestorsExpanded();
};

WordNode.prototype._getChildrenCount = function () {
    return this.nodeChildren.length;
};

WordNode.prototype._getLeftSibling = function () {
    return (this.leftNeighbor != null && this.leftNeighbor.nodeParent == this.nodeParent) ? this.leftNeighbor : null;
};

WordNode.prototype._getRightSibling = function () {
    return (this.rightNeighbor != null && this.rightNeighbor.nodeParent == this.nodeParent) ? this.rightNeighbor : null;
};

WordNode.prototype._getChildAt = function (i) {
	return this.nodeChildren[i];
};

WordNode.prototype._getChildrenCenter = function (tree) {
    var node = this._getFirstChild();
    var node1 = this._getLastChild();
    return node.prelim + ((node1.prelim - node.prelim) + tree._getNodeSize(node1)) / 2;	
};

WordNode.prototype._getFirstChild = function () {
	return this._getChildAt(0);
};

WordNode.prototype._getLastChild = function () {
	return this._getChildAt(this._getChildrenCount() - 1);
};

WordNode.prototype._drawChildrenLinks = function (tree) {
    var xb = 0, yb = 0, xc = 0, yc = 0, xd = 0, yd = 0;
    var node1 = null;
    var orientedRight = (tree.config.iRootOrientation == WordTree.RO_RIGHT);
    var ya = this.YPosition + (this.h / 2);
    var xa = this.XPosition + (orientedRight ? 0 : this.w);
    var t = tree.config.iLevelSeparation/2;

    for (var k = 0; k < this.nodeChildren.length; k++){
        node1 = this.nodeChildren[k];

        xd = node1.XPosition + (orientedRight ? node1.w : 0);
        yd = yc = node1.YPosition + (node1.h / 2);
        yb = ya;
        xb = xc = xd + (orientedRight ? 1 : -1)*t;

        var path = "M" + xa + " " + ya + "C" + xb + " " + yb + " " + xc + " " + yc + " " + xd + " " + yd; // smooth quadratic bezier curve
        var link = tree.paper.path(path).attr("stroke", tree.config.linkColor);
        node1.raphaelElements.paths.push(link);
    }
};

WordNode.prototype._setNodeColor = function(levelColors) {
    this.color = levelColors[this._getLevel() % levelColors.length];
};

WordNode.prototype._setNodeBorderColor = function(levelBorderColors) {
    return levelBorderColors[this._getLevel() % levelBorderColors.length];
};

WordNode.prototype.changeRaphaelElementVisibility = function(hide) {
    var i, els = this.raphaelElements, toChange = els.svgEls.concat(els.paths, els.collapseImg);

    for(i = 0; i < toChange.length; i++) {
        if (toChange[i] == null) continue;
        if (hide) {
            toChange[i].hide();
        } else {
            toChange[i].show();
        }
    }
};

function setConfig(configOptions) {
    var IMG_PATH = '../static/images/wordtree/';

    var config =  {
        iMaxDepth : 100,
        iLevelSeparation : 50,
        iSiblingSeparation : 2,
        iSubtreeSeparation : 2,
        iRootOrientation : WordTree.RO_LEFT,
        topXAdjustment : 0,
        topYAdjustment : 0,
        collapseAboveLevel : 1,
        linkColor : "#D68330",
        nodeColor : "#CCCCFF",
        nodeBorderColor : "blue",
        nodeSelColor : "#FFFFCC",
        levelColors : ["#5555FF","#8888FF","#AAAAFF","#CCCCFF"],
        levelBorderColors : ["#5555FF","#8888FF","#AAAAFF","#CCCCFF"],
        defaultNodeWidth  : 80,
        defaultNodeHeight : 40,
        expandedImage   : IMG_PATH + 'less.gif',
        collapsedImage  : IMG_PATH + 'plus.gif',
        transImage      : IMG_PATH + 'trans.gif',
        showLinksImage  : IMG_PATH + 'expand.png'
    };

    // customized options
    var configKeys = Object.keys(configOptions);
    for(var i = 0; i < configKeys.length; i++) {
        config[configKeys[i]] = configOptions[configKeys[i]];
    }

    return config;
}

WordTree = function (options) {
    var confOptions     = (options['confOptions'] == undefined) ? {} : options['confOptions'];
    var settingsOptions = (options['settings'] == undefined) ? {} : options['settings'];

    this.config = setConfig(confOptions);
    this.size = 24;
	this.self = this;
	
	this.maxLevelHeight = [];
	this.maxLevelWidth = [];
	this.previousLevelNode = [];
	
	this.rootYOffset = 0;
	this.rootXOffset = 0;
	
	this.nDatabaseNodes = [];
	this.mapIDs = {};

    // set custom options;
    var keys = Object.keys(settingsOptions);
    for(var i = 0; i < keys.length; i++) {
        this[keys[i]] = settingsOptions[keys[i]];
    }

	this.root = new WordNode({ id : -1, pid : null, dsc : null, w : 2, h : 2, size : this.size, target : null, paper : this.paper });
	this.iSelectedNode = -1;
    this.paperSet = this.paper.set();
    this.minY = 0;
    this.maxY = 0;
    this.rootElX = 0;
    this.rootElY = 0;
    this.rootNodeRaphaelElementId = this.orientedRight() ? WordTree.PREFIX_ROOT_NODE_RAPHAEL_EL_ID : WordTree.SUFFIX_ROOT_NODE_RAPHAEL_EL_ID;

    this.addJustDraggedEl();
    this.oppositeTree = null;
};

//Constant values
//Tree orientation
WordTree.RO_RIGHT = 0;
WordTree.RO_LEFT  = 1;

WordTree.PREFIX_ROOT_NODE_RAPHAEL_EL_ID = 'prefixRootNode';
WordTree.SUFFIX_ROOT_NODE_RAPHAEL_EL_ID = 'suffixRootNode';

WordTree.JUST_DRAGGED_RAPHAEL_EL_ID = 'justDraggedEl';
WordTree.JUST_DRAGGED_PROPERTY_NAME = 'justDragged';

WordTree.DEFAULT_FONT_SIZE   = 10;
WordTree.DEFAULT_FILL        = 'black';
WordTree.DEFAULT_FONT_WEIGHT = 'normal';
WordTree.DEFAULT_FONT_FAMILY = 'Cambria, serif';
WordTree.DEFAULT_TEXT_ANCHOR = 'start';
WordTree.ELS_TO_REMOVE_KEY   = 'elsToRemove';
WordTree.BOUNDING_RECT_KEY   = 'boundingRect';
WordTree.TRANS_IMG_KEY       = 'transImg';
WordTree.COLLAPSE_IMG_KEY    = 'collapseImg';
WordTree.SVG_ELS_KEY         = 'svgEls';
WordTree.PATHS_KEY           = 'paths';
WordTree.TEXT_KEY            = 'text';
WordTree.ORIG_FONT_SIZE_KEY  = 'orig-font-size';
WordTree.FILL_CSS_KEY        = 'fill';
WordTree.STROKE_CSS_KEY      = 'stroke';
WordTree.FONT_WEIGHT_CSS_KEY = 'font-weight';
WordTree.FONT_SIZE_CSS_KEY   = 'font-size';
WordTree.FONT_FAMILY_CSS_KEY = 'font-family';
WordTree.TEXT_ANCHOR_CSS_KEY = 'text-anchor';

WordTree.NODE_HIGHLIGHT_BORDER_COLOR = 'red';

WordTree.getJustDraggedEl = function(paper) {
    return paper.getById(WordTree.JUST_DRAGGED_RAPHAEL_EL_ID);
};

WordTree.prototype.addJustDraggedEl = function() {
    if (this.paper.getById(WordTree.JUST_DRAGGED_RAPHAEL_EL_ID) == null) {
        var rect = this.paper.rect(0, 0, 0, 0);
        rect.hide();
        rect.data(WordTree.JUST_DRAGGED_PROPERTY_NAME, false);
        rect.id = WordTree.JUST_DRAGGED_RAPHAEL_EL_ID;
    }
};

WordTree.wasJustDragged = function(paper) {
    var el = WordTree.getJustDraggedEl(paper);
    return el.data(WordTree.JUST_DRAGGED_PROPERTY_NAME) == true;
};

WordTree.setJustDraggedStatus = function(paper, status) {
    var el = WordTree.getJustDraggedEl(paper);
    el.data(WordTree.JUST_DRAGGED_PROPERTY_NAME, status);
};

WordTree.prototype.setOppositeTree = function(oppositeTree) {
    this.oppositeTree = oppositeTree;
};

//Layout algorithm
WordTree._firstWalk = function (tree, node, level) {
    var leftSibling = null;

    node.XPosition = 0;
    node.YPosition = 0;
    node.prelim = 0;
    node.modifier = 0;
    node.leftNeighbor = null;
    node.rightNeighbor = null;
    tree._setLevelHeight(node, level);
    tree._setLevelWidth(node, level);
    tree._setNeighbors(node, level);

    if (node._getChildrenCount() == 0 || level == tree.config.iMaxDepth) {
        leftSibling = node._getLeftSibling();
        node.prelim = (leftSibling == null) ? 0 :
            leftSibling.prelim + tree._getNodeSize(leftSibling) + tree.config.iSiblingSeparation;
    } else {
        var n = node._getChildrenCount();
        for(var i = 0; i < n; i++)
        {
            var iChild = node._getChildAt(i);
            WordTree._firstWalk(tree, iChild, level + 1);
        }

        var midPoint = node._getChildrenCenter(tree);
        midPoint -= tree._getNodeSize(node) / 2;
        leftSibling = node._getLeftSibling();

        if(leftSibling != null) {
            node.prelim = leftSibling.prelim + tree._getNodeSize(leftSibling) + tree.config.iSiblingSeparation;
            node.modifier = node.prelim - midPoint;
            WordTree._apportion(tree, node, level);
        } else {
            node.prelim = midPoint;
        }
    }
};

WordTree._apportion = function (tree, node, level) {
    var firstChild = node._getFirstChild();
    var firstChildLeftNeighbor = firstChild.leftNeighbor;
    var j = 1;
    for(var k = tree.config.iMaxDepth - level; firstChild != null && firstChildLeftNeighbor != null && j <= k;) {
        var modifierSumRight = 0, modifierSumLeft = 0;
        var rightAncestor = firstChild;
        var leftAncestor = firstChildLeftNeighbor;
        for(var l = 0; l < j; l++) {
            rightAncestor = rightAncestor.nodeParent;
            leftAncestor = leftAncestor.nodeParent;
            modifierSumRight += rightAncestor.modifier;
            modifierSumLeft += leftAncestor.modifier;
        }

        var totalGap = (firstChildLeftNeighbor.prelim + modifierSumLeft + tree._getNodeSize(firstChildLeftNeighbor) + tree.config.iSubtreeSeparation) - (firstChild.prelim + modifierSumRight);
        if (totalGap > 0) {
            var subtreeAux = node;
            var numSubtrees = 0;
            for(; subtreeAux != null && subtreeAux != leftAncestor; subtreeAux = subtreeAux._getLeftSibling())
                numSubtrees++;

            if (subtreeAux != null) {
                var subtreeMoveAux = node;
                var singleGap = totalGap / numSubtrees;
                for(; subtreeMoveAux != leftAncestor; subtreeMoveAux = subtreeMoveAux._getLeftSibling()) {
                    subtreeMoveAux.prelim += totalGap;
                    subtreeMoveAux.modifier += totalGap;
                    totalGap -= singleGap;
                }
            }
        }
        j++;
        firstChild = (firstChild._getChildrenCount() == 0) ? tree._getLeftmost(node, 0, j) : firstChild._getFirstChild();
        if (firstChild != null) {
            firstChildLeftNeighbor = firstChild.leftNeighbor;
        }
    }
};

WordTree._secondWalk = function (tree, node, level, X, Y) {
    if (level <= tree.config.iMaxDepth) {
        node.YPosition = tree.rootXOffset + node.prelim + X;
        node.XPosition = tree.rootYOffset + Y;

        if (tree.orientedRight()) {
            node.XPosition = -node.XPosition - node.w;
        }

        if(node._getChildrenCount() != 0) {
            WordTree._secondWalk(tree, node._getFirstChild(), level + 1, X + node.modifier, Y + tree.maxLevelWidth[level] + tree.config.iLevelSeparation);
        }
        var rightSibling = node._getRightSibling();
        if (rightSibling != null) {
            WordTree._secondWalk(tree, rightSibling, level, X, Y);
        }
    }
};

WordTree.prototype._positionTree = function () {
	this.maxLevelHeight     = [];
	this.maxLevelWidth      = [];
	this.previousLevelNode  = [];

	WordTree._firstWalk(this.self, this.root, 0);

    this.rootYOffset = this.config.topYAdjustment * (this.orientedRight() ? -1 : 1);
    this.rootXOffset = this.config.topXAdjustment;

	WordTree._secondWalk(this.self, this.root, 0, 0, 0);
};

WordTree.prototype._setLevelHeight = function (node, level) {
    if (this.maxLevelHeight[level] == null || this.maxLevelHeight[level] < node.h)
        this.maxLevelHeight[level] = node.h;	
};

WordTree.prototype._setLevelWidth = function (node, level) {
    if (this.maxLevelWidth[level] == null || this.maxLevelWidth[level] < node.w)
        this.maxLevelWidth[level] = node.w;		
};

WordTree.prototype._setNeighbors = function(node, level) {
    node.leftNeighbor = this.previousLevelNode[level];
    if (node.leftNeighbor != null)
        node.leftNeighbor.rightNeighbor = node;
    this.previousLevelNode[level] = node;	
};

WordTree.prototype._getNodeSize = function (node) {
    return node.h;
};

WordTree.prototype._getLeftmost = function (node, level, maxlevel) {
    var n = node._getChildrenCount();

    if (level >= maxlevel) return node;
    if (n == 0) return null;

    for(var i = 0; i < n; i++) {
        var iChild = node._getChildAt(i);
        var leftmostDescendant = this._getLeftmost(iChild, level + 1, maxlevel);
        if (leftmostDescendant != null)
            return leftmostDescendant;
    }

    return null;	
};

WordTree.prototype.getNodeFromNodeId = function(nodeId) {
    return this.nDatabaseNodes[this.mapIDs[nodeId]];
};

WordTree.prototype.selectNode = function (nodeId, rect) {
    var bRect   = rect.data(WordTree.BOUNDING_RECT_KEY);
    var dbIndex = this.mapIDs[nodeId];
    var node    = this.nDatabaseNodes[dbIndex];

    node.isSelected = !node.isSelected;

    if (this.oppositeTree.iSelectedNode != -1) {
        unhighlightNode(this.oppositeTree);
    }
    if ((this.iSelectedNode == dbIndex && !node.isSelected) || this.iSelectedNode != -1) {
        unhighlightNode(this);
    }

    if (node.isSelected) {
        highlight(bRect);
        this.iSelectedNode = dbIndex;
    }
};

WordTree.prototype.collapseAboveLevel = function() {
    var n, node;
    for (n = 0; n < this.nDatabaseNodes.length; n++) {
        node = this.nDatabaseNodes[n];
        if (node._getLevel() > this.collapseAboveLevel && node.isCollapsed == false) {
            node.collapseNode(node.id, node.id);
        }
    }
};

WordTree.prototype._drawTree = function () {
    const nodeRectRadius = 8, nodePadX = 10, nodePadY = 12, nodeHOffset = 12, nodeWOffset = 7, expandImgWidth = 15;
    var node = null;

    function onClickSelect() {
        var tree = this.data('tree');
        var node = this.data('node');
        if (tree == undefined || node == undefined) return;
        tree.selectNode(node.id, this);
    }

    function onClickCollapse() {
        var tree = this.data('tree');
        var node = this.data('node');
        tree.collapseNode(node.id, node.id);
    }

    this.paper.setStart();

	for (var n = 0; n < this.nDatabaseNodes.length; n++) {
		node = this.nDatabaseNodes[n];
        if (!node._isAncestorCollapsed()) {

            node._setNodeBorderColor(this.config.levelBorderColors);
            node._setNodeColor(this.config.levelColors);

            var textAttrs = { fill : node.color },
                anchorRectAttrs = {
                    x : node.XPosition, y : node.YPosition, radius : nodeRectRadius,
                    hOffset: nodeHOffset, wOffset : nodeWOffset + expandImgWidth,
                    fill : 'white', stroke : node.borderColor },
                anchorData = { 'tree' : this, 'node' : node },
                textX = node.XPosition + 5,
                textY = node.YPosition + node.h/2;

            textAttrs[WordTree.FONT_SIZE_CSS_KEY] = node.size;
            var els = this._addAnchorRect(this.paper, textX, textY, node.dsc, textAttrs, anchorData, anchorRectAttrs, onClickSelect);
            var text = els.text, aRect = els.anchor, bRect = els[WordTree.BOUNDING_RECT_KEY],
                textBBox = text.getBBox(), rectBBox = aRect.getBBox();

            if (rectBBox.y < this.minY)  this.minY = rectBBox.y;
            if (rectBBox.y2 > this.maxY) this.maxY = rectBBox.y2;
            if (n == 0) {
                aRect.id = this.rootNodeRaphaelElementId;
            }

            if (node.target) {
                els.items.push(this._addTargetOverlays(node.target, textBBox));
            }

            node.w = textBBox.width + nodePadX + expandImgWidth;
            node.h = textBBox.height + nodePadY;
            node.raphaelElements[WordTree.SVG_ELS_KEY] = node.raphaelElements[WordTree.SVG_ELS_KEY].concat(els.items);
            node.raphaelElements[WordTree.BOUNDING_RECT_KEY] = bRect;

            if (node.canCollapse) {
                var collapseImg = this.paper.image((node.isCollapsed) ? this.config.collapsedImage : this.config.expandedImage,
                                            node.XPosition, node.YPosition, 10, 10);
                var transImg = this.paper.image(this.config.transImage, node.XPosition, node.YPosition, 10, 10)
                    .data('tree', this)
                    .data('node', node)
                    .click(onClickCollapse);
                node.raphaelElements[WordTree.COLLAPSE_IMG_KEY] = collapseImg;
                node.raphaelElements[WordTree.TRANS_IMG_KEY]    = transImg;
            }

            if (!node.isCollapsed) {
                node._drawChildrenLinks(this.self);
            }
        }
	}
    this.paperSet = this.paper.setFinish();
};



WordTree.prototype._getText = function(paper, x, y, desc, textAttrs) {
    var t = paper.text(x, y, desc);
    t.attr(WordTree.TEXT_ANCHOR_CSS_KEY, WordTree.DEFAULT_TEXT_ANCHOR)
     .attr(WordTree.FONT_SIZE_CSS_KEY,   WordTree.DEFAULT_FONT_SIZE)
     .attr(WordTree.FONT_FAMILY_CSS_KEY, WordTree.DEFAULT_FONT_FAMILY);

    return _setElementProperties(t, textAttrs, 'attr');
};

WordTree.prototype._addAnchorRect = function(paper, x, y, desc, textAttrs, anchorData,
                                             boundingRectAttrs, onClickHandler) {
    const RECT_PAD_X_DEFAULT = 2;

    var aRect = null, bRect = null,
        text  = this._getText(paper, x, y, desc, textAttrs),
        bbox  = text.getBBox();

    var drawBoundingRect = true;
    if (boundingRectAttrs == null) {
        drawBoundingRect = false;
        boundingRectAttrs = {};
    }

    var bx = returnIfDefined(boundingRectAttrs.x, bbox.x - RECT_PAD_X_DEFAULT),
        by = returnIfDefined(boundingRectAttrs.y, bbox.y),
        bw = returnIfDefined(boundingRectAttrs.width, bbox.width + 2*RECT_PAD_X_DEFAULT) + returnIfDefined(boundingRectAttrs.wOffset, 0),
        bh = returnIfDefined(boundingRectAttrs.height, bbox.height) + returnIfDefined(boundingRectAttrs.hOffset, 0),
        br = returnIfDefined(boundingRectAttrs.r, 3);
    var items = [];

    if (drawBoundingRect) {
        var bfill = returnIfDefined(boundingRectAttrs.fill, 'lightgray'),
            bsc   = returnIfDefined(boundingRectAttrs.stroke, 'gray');
        bRect = this.paper.rect(bx, by, bw, bh, br).attr( { 'fill': bfill, 'stroke' : bsc });
        items.push(bRect);
    }

    aRect = this.paper.rect(bx, by, bw, bh, br)
                      .attr('opacity', 0).attr('fill', 'black').attr('cursor', 'pointer')
                      .data(WordTree.BOUNDING_RECT_KEY, bRect);
    items.push(text, aRect);

    anchorData[WordTree.TEXT_KEY] = text;
    if (anchorData[WordTree.ELS_TO_REMOVE_KEY] != undefined) {
        anchorData[WordTree.ELS_TO_REMOVE_KEY] = anchorData[WordTree.ELS_TO_REMOVE_KEY].concat(items);
    }
    aRect = _setElementProperties(aRect, anchorData, 'data');
    aRect.mouseup(onClickHandler);

    function changeTextColor(e) {
        var txt = this.data(WordTree.TEXT_KEY);
        if (txt == undefined || txt.node == undefined) return;

        var keysAndDefaults = [ { key : WordTree.FILL_CSS_KEY, default : WordTree.DEFAULT_FILL },
                                { key : WordTree.FONT_WEIGHT_CSS_KEY, default : WordTree.DEFAULT_FONT_WEIGHT },
                                { key : WordTree.FONT_SIZE_CSS_KEY, default : returnIfDefined(this.data(WordTree.ORIG_FONT_SIZE_KEY), WordTree.DEFAULT_FONT_SIZE) }];
        for(var i = 0; i < keysAndDefaults.length; i++) {
            var key = keysAndDefaults[i].key, defaultVal = keysAndDefaults[i].default;

            if (this.data(key) != undefined) {
                txt.attr(key, (e.type == 'mouseover') ? this.data(key) : defaultVal);
            }
        }
    }
    aRect.hover(changeTextColor, changeTextColor);

    text.toFront();
    aRect.toFront();

    return { text : text, anchor : aRect, boundingRect : bRect, items : items };
};

WordTree.prototype._addTargetOverlays = function (target, textBBox) {
    const imgSize = 15, imgX = textBBox.x2 + 5, imgY = textBBox.y + (textBBox.height - imgSize)/2;

    var viewDocUrl = this.viewDocUrl,
              tree = this;

    return this.paper.image(this.config.showLinksImage, imgX, imgY, imgSize, imgSize)
                     .data('paper', this.paper)
                     .data('target', target)
                     .click(anchorClick);

    function linkClickHandler() {
        window.open(viewDocUrl + "&id=" + this.data('id'), "_blank");
    }

    function closeAnchorClick() {
        this.data(WordTree.ELS_TO_REMOVE_KEY).applyFn('remove');
    }

    function anchorClick () {
        var paper = this.data('paper');

        if (WordTree.wasJustDragged(paper)) {
            WordTree.setJustDraggedStatus(paper, false);
            return;
        }

        var bbox = this.getBBox();
        var tgt  = this.data('target');
        var i, rectPadX = 10, rectPadY = 10, fontSize = 10, textPadY = 5, rectX = bbox.x, rectY = bbox.y;
        var links = [], headerAttrs = {};

        headerAttrs[WordTree.FONT_SIZE_CSS_KEY]   = fontSize;
        headerAttrs[WordTree.FONT_WEIGHT_CSS_KEY] = 'bold';
        var header = tree._getText(paper, rectX + rectPadX*2, rectY + rectPadY, "Files with this sentence:", headerAttrs);

        links.push(header);
        var textAttrs = {"font-size" : fontSize},
                textX = rectX + rectPadX,
                textY = rectY + rectPadY* 2,
                    w = header.getBBox().width + rectPadY;

        for(i = 0; i < tgt.length; i++) {
            var desc = tgt[i]['title'],
                anchorData = { id : tgt[i]['id'] };
            anchorData[WordTree.FILL_CSS_KEY]        = 'blue';
            anchorData[WordTree.FONT_WEIGHT_CSS_KEY] = 'bold';

            var newEls = tree._addAnchorRect(paper, textX, textY + (i + 1)*(fontSize + textPadY), desc, textAttrs,
                                             anchorData, null, linkClickHandler);
            links = links.concat(newEls.items);

            w = Math.max(newEls.anchor.getBBox().width, w);
        }

        var y2 = links[links.length - 1].getBBox().y2 - rectY + rectPadY;
        var linksRect = paper.rect(rectX, rectY, w + rectPadX*2, y2, 8)
                             .attr('fill', 'white').attr('stroke', 'black');

        anchorData = {};
        anchorData[WordTree.FONT_SIZE_CSS_KEY]      = 12;
        anchorData[WordTree.FONT_ORIG_SIZE_CSS_KEY] = 10;
        anchorData[WordTree.FONT_WEIGHT_CSS_KEY]    = 'bold';

        anchorData[WordTree.ELS_TO_REMOVE_KEY] = links.concat(linksRect);
        var closeEls = tree._addAnchorRect(paper, rectX + rectPadX/2 + 3, rectY + rectPadY/2 + 8, "X", textAttrs,
                                           anchorData, {}, closeAnchorClick);
        links.concat(closeEls.items).applyFn('toFront');
    }
};

WordTree.prototype._removeRaphaelEls = function () {
    this.paperSet.remove();
};

WordTree.prototype._transformPrefixTree = function() {
    if (!this.orientedRight()) return;

    var suffixRootNode = this.paper.getById(WordTree.SUFFIX_ROOT_NODE_RAPHAEL_EL_ID);
    var prefixRootNode = this.paper.getById(WordTree.PREFIX_ROOT_NODE_RAPHAEL_EL_ID);
    if (suffixRootNode == undefined || prefixRootNode == undefined) return;

    var xdiff = suffixRootNode.attr("x") - prefixRootNode.attr("x");// - prefixRootNode.attr("width") - 5;
    var ydiff = suffixRootNode.attr("y") - prefixRootNode.attr("y");

    this.paperSet.transform("t" + xdiff + "," + ydiff);
};

WordTree.prototype._setRootElementCoords = function() {
    var el = this.paper.getById(this.rootNodeRaphaelElementId);
    this.rootElX = el.attr('x');
    this.rootElY = el.attr('y');
};

// WordTree API begins here...
WordTree.prototype.UpdateTree = function () {
    var el = this.paper.getById(this.rootNodeRaphaelElementId);
    if (el != undefined) {
        this.config.topYAdjustment += (el.attr('x') - this.rootElX);
        this.config.topXAdjustment += (el.attr('y') - this.rootElY);
    }

    this._removeRaphaelEls();
    this._positionTree();
    this._drawTree();
    this._transformPrefixTree();
    this._setRootElementCoords();
};

WordTree.prototype.orientedRight = function() {
    return this.config.iRootOrientation == WordTree.RO_RIGHT;
};

WordTree.prototype.add = function (id, pid, dsc, size, target) {
	var pnode = null; //Search for parent node in database
	if (pid == -1) {
	    pnode = this.root;
	} else {
        for (var k = 0; k < this.nDatabaseNodes.length; k++) {
		    if (this.nDatabaseNodes[k].id == pid) {
			    pnode = this.nDatabaseNodes[k];
				break;
			}
		}
	}

	var node = new WordNode({ id : id, pid : pid, dsc : dsc, w : this.config.defaultNodeWidth, h : this.config.defaultNodeHeight,
        size : size, target : target, paper : this.paper });
	node.nodeParent = pnode;  //Set it's parent
	pnode.canCollapse = true; //It's obvious that now the parent can collapse	
	var i = this.nDatabaseNodes.length;	//Save it in database
	node.dbIndex = this.mapIDs[id] = i;	 
	this.nDatabaseNodes[i] = node;	

	node.siblingIndex = pnode.nodeChildren.length;     //Add it as child of it's parent
	pnode.nodeChildren[node.siblingIndex] = node;
    return node;
};

WordTree.prototype.search = function(str, fromSel) {
    if (str == '') return [];

    var nodeId = fromSel ? this.iSelectedNode : 0;
    return this.searchChildren(str, fromSel, nodeId, []);
};

WordTree.prototype.searchChildren = function(str, fromSel, nodeId, nodesFound) {
    var node = this.nDatabaseNodes[this.mapIDs[nodeId]];
    var nodeChildren = node.nodeChildren;

    if (node.dsc.match(str)) {
        nodesFound.push(node);
    }
    for (var i = 0; i < nodeChildren.length; i++) {
        this.searchChildren(str, fromSel, nodeChildren[i].id, nodesFound);
    }
    return nodesFound;
};


WordTree.prototype.collapseNode = function (nodeId, startNodeId) {
    var node = this.nDatabaseNodes[this.mapIDs[nodeId]];
    var nodeChildren = node.nodeChildren;
	node.isCollapsed = !node.isCollapsed;

    if (nodeId != startNodeId) {
        node.changeRaphaelElementVisibility(node.isCollapsed);
    } else {
        node.raphaelElements[WordTree.COLLAPSE_IMG_KEY].attr('src',
            (node.isCollapsed) ? this.config.collapsedImage : this.config.expandedImage);
    }

    for (var i = 0; i < nodeChildren.length; i++) {
        this.collapseNode(nodeChildren[i].id, startNodeId);
    }
};