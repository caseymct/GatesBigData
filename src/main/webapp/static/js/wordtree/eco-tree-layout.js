ECONode = function (id, pid, dsc, w, h, c, bc, size, target, meta, paper) {
	this.id = id;
	this.pid = pid;
	this.dsc = dsc;
	this.w = w;
	this.h = h;
	this.c = c;
	this.bc = bc;
	this.target = target;
	this.meta = meta;
	
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
    this.paper = paper;
    this.size = size;
};

ECONode.prototype._getLevel = function () {
	if (this.nodeParent.id == -1) {return 0;}
	else return this.nodeParent._getLevel() + 1;
};

ECONode.prototype._isAncestorCollapsed = function () {
	if (this.nodeParent.isCollapsed) return true;
    if (this.nodeParent.id == -1) return false;

	return this.nodeParent._isAncestorCollapsed();
};

ECONode.prototype._setAncestorsExpanded = function () {
	if (this.nodeParent.id == -1) return;

	this.nodeParent.isCollapsed = false;
    return this.nodeParent._setAncestorsExpanded();
};

ECONode.prototype._getChildrenCount = function () {
    return this.nodeChildren.length;

    //return (this.isCollapsed || this.nodeChildren == null) ? 0 : this.nodeChildren.length;
};

ECONode.prototype._getLeftSibling = function () {
    return (this.leftNeighbor != null && this.leftNeighbor.nodeParent == this.nodeParent) ? this.leftNeighbor : null;
};

ECONode.prototype._getRightSibling = function () {
    return (this.rightNeighbor != null && this.rightNeighbor.nodeParent == this.nodeParent) ? this.rightNeighbor : null;
};

ECONode.prototype._getChildAt = function (i) {
	return this.nodeChildren[i];
};

ECONode.prototype._getChildrenCenter = function (tree) {
    var node = this._getFirstChild();
    var node1 = this._getLastChild();
    return node.prelim + ((node1.prelim - node.prelim) + tree._getNodeSize(node1)) / 2;	
};

ECONode.prototype._getFirstChild = function () {
	return this._getChildAt(0);
};

ECONode.prototype._getLastChild = function () {
	return this._getChildAt(this._getChildrenCount() - 1);
};

ECONode.prototype._drawChildrenLinks = function (tree) {
    var xb = 0, yb = 0, xc = 0, yc = 0, xd = 0, yd = 0;
    var node1 = null;
    var orientedRight = (tree.config.iRootOrientation == ECOTree.RO_RIGHT);
    var ya = this.YPosition + (this.h / 2);
    var xa = this.XPosition + (orientedRight ? 0 : this.w);
    var t = tree.config.iLevelSeparation/2;

    for (var k = 0; k < this.nodeChildren.length; k++){
        node1 = this.nodeChildren[k];

        xd = node1.XPosition + (orientedRight ? node1.w : 0);
        yd = yc = node1.YPosition + (node1.h / 2);
        yb = ya;

        switch (tree.config.iNodeJustification) {
            case ECOTree.NJ_TOP:
                xb = xc = xd + (orientedRight ? 1 : -1)*t;
                break;
            case ECOTree.NJ_BOTTOM:
                xb = xc = xa + (orientedRight ? -1 : 1)*t;
                break;
            case ECOTree.NJ_CENTER:
                if (orientedRight) {
                    xb = xc = xd + (xa - xd) / 2;
                } else {
                    xb = xc = xa + (xd - xa) / 2;
                }
                break;
        }

        var path = "M" + xa + " " + ya + "C" + xb + " " + yb + " " + xc + " " + yc + " " + xd + " " + yd; // smooth quadratic bezier curve
        var link = tree.paper.path(path).attr("stroke", tree.config.linkColor);
        tree.raphaelElements.push(link);
    }
};

function setConfig(configOptions) {
    var config =  {
        iMaxDepth : 100,
        iLevelSeparation : 80,
        iSiblingSeparation : 5,
        iSubtreeSeparation : 10,
        iRootOrientation : ECOTree.RO_LEFT,
        iNodeJustification : ECOTree.NJ_TOP,
        topXAdjustment : 0,
        topYAdjustment : 0,
        linkType : "B",
        linkColor : "#D68330",
        nodeColor : "#CCCCFF",
        nodeBorderColor : "blue",
        nodeSelColor : "#FFFFCC",
        levelColors : ["#5555FF","#8888FF","#AAAAFF","#CCCCFF"],
        levelBorderColors : ["#5555FF","#8888FF","#AAAAFF","#CCCCFF"],
        colorStyle : ECOTree.CS_NODE,
        useTarget : true,
        searchMode : ECOTree.SM_DSC,
        selectMode : ECOTree.SL_MULTIPLE,
        defaultNodeWidth : 80,
        defaultNodeHeight : 40,
        defaultTarget : 'javascript:void(0);',
        expandedImage : './img/less.gif',
        collapsedImage : './img/plus.gif',
        transImage : './img/trans.gif'
    };

    // customized options
    var configKeys = Object.keys(configOptions);
    for(var i = 0; i < configKeys.length; i++) {
        config[configKeys[i]] = configOptions[configKeys[i]];
    }

    return config;
}

ECOTree = function (options) {
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

	this.root = new ECONode(-1, null, null, 2, 2, "black", "none", this.size, null, null, this.paper);
	this.iSelectedNode = -1;
	this.iLastSearch = 0;
    this.raphaelElements = [];
    this.rootNodeRaphaelElement = null;
};

//Constant values

//Tree orientation
ECOTree.RO_TOP = 0;
ECOTree.RO_BOTTOM = 1;
ECOTree.RO_RIGHT = 2;
ECOTree.RO_LEFT = 3;

//Level node alignment
ECOTree.NJ_TOP = 0;
ECOTree.NJ_CENTER = 1;
ECOTree.NJ_BOTTOM = 2;

//Node fill type
ECOTree.NF_GRADIENT = 0;
ECOTree.NF_FLAT = 1;

//Colorizing style
ECOTree.CS_NODE = 0;
ECOTree.CS_LEVEL = 1;

//Search method: Title, metadata or both
ECOTree.SM_DSC = 0;
ECOTree.SM_META = 1;
ECOTree.SM_BOTH = 2;

//Selection mode: single, multiple, no selection
ECOTree.SL_MULTIPLE = 0;
ECOTree.SL_SINGLE = 1;
ECOTree.SL_NONE = 2;

//Layout algorithm
ECOTree._firstWalk = function (tree, node, level) {
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
            ECOTree._firstWalk(tree, iChild, level + 1);
        }

        var midPoint = node._getChildrenCenter(tree);
        midPoint -= tree._getNodeSize(node) / 2;
        leftSibling = node._getLeftSibling();

        if(leftSibling != null) {
            node.prelim = leftSibling.prelim + tree._getNodeSize(leftSibling) + tree.config.iSiblingSeparation;
            node.modifier = node.prelim - midPoint;
            ECOTree._apportion(tree, node, level);
        } else {
            node.prelim = midPoint;
        }
    }
};

ECOTree._apportion = function (tree, node, level) {
        var firstChild = node._getFirstChild();
        var firstChildLeftNeighbor = firstChild.leftNeighbor;
        var j = 1;
        for(var k = tree.config.iMaxDepth - level; firstChild != null && firstChildLeftNeighbor != null && j <= k;)
        {
            var modifierSumRight = 0;
            var modifierSumLeft = 0;
            var rightAncestor = firstChild;
            var leftAncestor = firstChildLeftNeighbor;
            for(var l = 0; l < j; l++)
            {
                rightAncestor = rightAncestor.nodeParent;
                leftAncestor = leftAncestor.nodeParent;
                modifierSumRight += rightAncestor.modifier;
                modifierSumLeft += leftAncestor.modifier;
            }

            var totalGap = (firstChildLeftNeighbor.prelim + modifierSumLeft + tree._getNodeSize(firstChildLeftNeighbor) + tree.config.iSubtreeSeparation) - (firstChild.prelim + modifierSumRight);
            if(totalGap > 0)
            {
                var subtreeAux = node;
                var numSubtrees = 0;
                for(; subtreeAux != null && subtreeAux != leftAncestor; subtreeAux = subtreeAux._getLeftSibling())
                    numSubtrees++;

                if(subtreeAux != null)
                {
                    var subtreeMoveAux = node;
                    var singleGap = totalGap / numSubtrees;
                    for(; subtreeMoveAux != leftAncestor; subtreeMoveAux = subtreeMoveAux._getLeftSibling())
                    {
                        subtreeMoveAux.prelim += totalGap;
                        subtreeMoveAux.modifier += totalGap;
                        totalGap -= singleGap;
                    }

                }
            }
            j++;
            if(firstChild._getChildrenCount() == 0)
                firstChild = tree._getLeftmost(node, 0, j);
            else
                firstChild = firstChild._getFirstChild();
            if(firstChild != null)
                firstChildLeftNeighbor = firstChild.leftNeighbor;
        }
};

ECOTree._secondWalk = function (tree, node, level, X, Y) {

    if (level <= tree.config.iMaxDepth) {
        node.YPosition = tree.rootXOffset + node.prelim + X;
        node.XPosition = tree.rootYOffset + Y;

        if (tree.orientedRight()) {
            node.XPosition = -node.XPosition - node.w;
        }

        if(node._getChildrenCount() != 0) {
            ECOTree._secondWalk(tree, node._getFirstChild(), level + 1, X + node.modifier, Y + tree.maxLevelWidth[level] + tree.config.iLevelSeparation);
        }
        var rightSibling = node._getRightSibling();
        if (rightSibling != null) {
            ECOTree._secondWalk(tree, rightSibling, level, X, Y);
        }
    }
};

ECOTree.prototype._positionTree = function () {
	this.maxLevelHeight     = [];
	this.maxLevelWidth      = [];
	this.previousLevelNode  = [];

	ECOTree._firstWalk(this.self, this.root, 0);

    this.rootYOffset = this.config.topYAdjustment * (this.orientedRight() ? -1 : 1);
    this.rootXOffset = this.config.topXAdjustment;

	ECOTree._secondWalk(this.self, this.root, 0, 0, 0);
};

ECOTree.prototype._setLevelHeight = function (node, level) {	
	if (this.maxLevelHeight[level] == null) 
		this.maxLevelHeight[level] = 0;
    if(this.maxLevelHeight[level] < node.h)
        this.maxLevelHeight[level] = node.h;	
};

ECOTree.prototype._setLevelWidth = function (node, level) {
	if (this.maxLevelWidth[level] == null) 
		this.maxLevelWidth[level] = 0;
    if(this.maxLevelWidth[level] < node.w)
        this.maxLevelWidth[level] = node.w;		
};

ECOTree.prototype._setNeighbors = function(node, level) {
    node.leftNeighbor = this.previousLevelNode[level];
    if(node.leftNeighbor != null)
        node.leftNeighbor.rightNeighbor = node;
    this.previousLevelNode[level] = node;	
};

ECOTree.prototype._getNodeSize = function (node) {
    return node.h;
};

ECOTree.prototype._getLeftmost = function (node, level, maxlevel) {
    if(level >= maxlevel) return node;
    if(node._getChildrenCount() == 0) return null;
    
    var n = node._getChildrenCount();
    for(var i = 0; i < n; i++)
    {
        var iChild = node._getChildAt(i);
        var leftmostDescendant = this._getLeftmost(iChild, level + 1, maxlevel);
        if(leftmostDescendant != null)
            return leftmostDescendant;
    }

    return null;	
};

ECOTree.prototype._selectNodeInt = function (dbindex, flagToggle) {
	if (this.config.selectMode == ECOTree.SL_SINGLE) {
		if ((this.iSelectedNode != dbindex) && (this.iSelectedNode != -1)) {
			this.nDatabaseNodes[this.iSelectedNode].isSelected = false;
		}		
		this.iSelectedNode = (this.nDatabaseNodes[dbindex].isSelected && flagToggle) ? -1 : dbindex;
	}	
	this.nDatabaseNodes[dbindex].isSelected = (flagToggle) ? !this.nDatabaseNodes[dbindex].isSelected : true;	
};

ECOTree.prototype._collapseAllInt = function (flag) {
	var node = null;
	for (var n = 0; n < this.nDatabaseNodes.length; n++)
	{ 
		node = this.nDatabaseNodes[n];
		if (node.canCollapse) node.isCollapsed = flag;
	}	
	this.UpdateTree();
};

ECOTree.prototype._selectAllInt = function (flag) {
	var node = null;
	for (var k = 0; k < this.nDatabaseNodes.length; k++) {
		node = this.nDatabaseNodes[k];
		node.isSelected = flag;
	}

	this.iSelectedNode = -1;
	this.UpdateTree();
};

ECOTree.prototype._drawTree = function () {
	var node = null;
	var color = "", border = "";
    var n, rect, text;
    var set = [];

	for (n = 0; n < this.nDatabaseNodes.length; n++) {
		node = this.nDatabaseNodes[n];
		
		switch (this.config.colorStyle) {
			case ECOTree.CS_NODE:
				color = node.c;
				border = node.bc;
				break;
			case ECOTree.CS_LEVEL:
				var iColor = node._getLevel() % this.config.levelColors.length;
				color = this.config.levelColors[iColor];
				iColor = node._getLevel() % this.config.levelBorderColors.length;
				border = this.config.levelBorderColors[iColor];
				break;
		}

        if (!node._isAncestorCollapsed()) {
            set = this.paper.set();
            text = this.paper.text(node.XPosition + 5, node.YPosition + node.h/2, node.dsc);
            text.attr("font-size", node.size);
            text.attr("text-anchor", "start");

            if(node._getChildrenCount() == 0){
                text.attr("fill", "#888");
            }

            node.w = text.getBBox().width + 10;
            node.h = text.getBBox().height + 12;
            rect = this.paper.rect(node.XPosition, node.YPosition, node.w, node.h, 8).attr({
                fill : "none",
                stroke : node.c
            });

            if (n == 0) {
                this.rootNodeRaphaelElement = rect;
            }
            this.raphaelElements.push(rect, text);

            if (node.canCollapse) {
                var imgSrc = (node.isCollapsed) ? this.config.collapsedImage : this.config.expandedImage;
                var img1 = this.paper.image(imgSrc, node.XPosition, node.YPosition, 10, 10);
                var img2 = this.paper.image(this.config.transImage, node.XPosition, node.YPosition, node.w, node.h)
                    .data("tree", this)
                    .data("node", node)
                    .click(function() {
                        var tree = this.data("tree");
                        var node = this.data("node");
                        tree.collapseNode(node.id, true);
                    });

                this.raphaelElements.push(img1, img2);
            }

            if (node.target) {
                var anchor = this.paper.rect().attr(text.getBBox()).attr({
                    fill: "#000",
                    opacity: 0,
                    cursor: "pointer"
                });
                anchor.data("target", node.target);
                anchor.data("text", text);
                anchor.click(function () {
                    var tgt = this.data("target");
                    debugger;
                });

                anchor.hover(function() { this.data("text").attr("fill", "blue" ) },
                             function() { this.data("text").attr("fill", "black") });
                this.raphaelElements.push(anchor);
            }
            if (!node.isCollapsed) {
                node._drawChildrenLinks(this.self);
            }
        }
	}	

};

ECOTree.prototype._removeRaphaelEls = function () {
    for(var i = 0; i < this.raphaelElements.length; i++) {
        this.raphaelElements[i].remove();
    }
    this.raphaelElements = [];
};

ECOTree.prototype._transformPrefixTree = function(suffixRootNode) {
    if (suffixRootNode == undefined || !this.orientedRight()) {
        return;
    }

    var xdiff = suffixRootNode.attr("x") - this.rootNodeRaphaelElement.attr("x");
    var ydiff = suffixRootNode.attr("y") - this.rootNodeRaphaelElement.attr("y");

    for(var i = 0; i < this.raphaelElements.length; i++) {
        this.raphaelElements[i].transform("t" + xdiff + "," + ydiff);
    }
};

// ECOTree API begins here...
ECOTree.prototype.UpdateTree = function (suffixRootNode) {
    this._removeRaphaelEls();
    this._positionTree();
    this._drawTree();
    this._transformPrefixTree(suffixRootNode);
};

ECOTree.prototype.orientedRight = function() {
    return this.config.iRootOrientation == ECOTree.RO_RIGHT;
};

ECOTree.prototype.add = function (id, pid, dsc, size, c, bc, target, meta) {
	var color = c || this.config.nodeColor;
	var border = bc || this.config.nodeBorderColor;
	var tg = (this.config.useTarget) ? ((typeof target == "undefined") ? (this.config.defaultTarget) : target) : null;
	var metadata = (typeof meta != "undefined")	? meta : "";
	
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
	
	var node = new ECONode(id, pid, dsc, this.config.defaultNodeWidth, this.config.defaultNodeHeight,
        color, border, size, tg, metadata, this.paper);	//New node creation...
	node.nodeParent = pnode;  //Set it's parent
	pnode.canCollapse = true; //It's obvious that now the parent can collapse	
	var i = this.nDatabaseNodes.length;	//Save it in database
	node.dbIndex = this.mapIDs[id] = i;	 
	this.nDatabaseNodes[i] = node;	

	node.siblingIndex = pnode.nodeChildren.length;     //Add it as child of it's parent
	pnode.nodeChildren[node.siblingIndex] = node;
    return node;
};

ECOTree.prototype.searchNodes = function (str) {
	var node = null;
	var m = this.config.searchMode;
	var sm = (this.config.selectMode == ECOTree.SL_SINGLE);	 
	
	if (typeof str == "undefined") return;
	if (str == "") return;
	
	var found = false;
	var n = (sm) ? this.iLastSearch : 0;
	if (n == this.nDatabaseNodes.length) n = this.iLastSeach = 0;
	
	str = str.toLocaleUpperCase();
	
	for (; n < this.nDatabaseNodes.length; n++)
	{ 		
		node = this.nDatabaseNodes[n];				
		if (node.dsc.toLocaleUpperCase().indexOf(str) != -1 && ((m == ECOTree.SM_DSC) || (m == ECOTree.SM_BOTH))) { node._setAncestorsExpanded(); this._selectNodeInt(node.dbIndex, false); found = true; }
		if (node.meta.toLocaleUpperCase().indexOf(str) != -1 && ((m == ECOTree.SM_META) || (m == ECOTree.SM_BOTH))) { node._setAncestorsExpanded(); this._selectNodeInt(node.dbIndex, false); found = true; }
		if (sm && found) {this.iLastSearch = n + 1; break;}
	}	
	this.UpdateTree();	
};

ECOTree.prototype.selectAll = function () {
	if (this.config.selectMode != ECOTree.SL_MULTIPLE) return;
	this._selectAllInt(true);
};

ECOTree.prototype.unselectAll = function () {
	this._selectAllInt(false);
};

ECOTree.prototype.collapseAll = function () {
	this._collapseAllInt(true);
};

ECOTree.prototype.expandAll = function () {
	this._collapseAllInt(false);
};

ECOTree.prototype.collapseNode = function (nodeid, upd) {
	var dbindex = this.mapIDs[nodeid];
	this.nDatabaseNodes[dbindex].isCollapsed = !this.nDatabaseNodes[dbindex].isCollapsed;
	if (upd) this.UpdateTree();
};

ECOTree.prototype.selectNode = function (nodeid, upd) {		
	this._selectNodeInt(this.mapIDs[nodeid], true);
	if (upd) this.UpdateTree();
};

ECOTree.prototype.setNodeTitle = function (nodeid, title, upd) {
	var dbindex = this.mapIDs[nodeid];
	this.nDatabaseNodes[dbindex].dsc = title;
	if (upd) this.UpdateTree();
};

ECOTree.prototype.setNodeMetadata = function (nodeid, meta, upd) {
	var dbindex = this.mapIDs[nodeid];
	this.nDatabaseNodes[dbindex].meta = meta;
	if (upd) this.UpdateTree();
};

ECOTree.prototype.setNodeTarget = function (nodeid, target, upd) {
	var dbindex = this.mapIDs[nodeid];
	this.nDatabaseNodes[dbindex].target = target;
	if (upd) this.UpdateTree();	
}

ECOTree.prototype.setNodeColors = function (nodeid, color, border, upd) {
	var dbindex = this.mapIDs[nodeid];
	if (color) this.nDatabaseNodes[dbindex].c = color;
	if (border) this.nDatabaseNodes[dbindex].bc = border;
	if (upd) this.UpdateTree();	
}

ECOTree.prototype.getSelectedNodes = function () {
	var node = null;
	var selection = [];
	var selnode = null;	
	
	for (var n=0; n<this.nDatabaseNodes.length; n++) {
		node = this.nDatabaseNodes[n];
		if (node.isSelected)
		{			
			selnode = {
				"id" : node.id,
				"dsc" : node.dsc,
				"meta" : node.meta
			}
			selection[selection.length] = selnode;
		}
	}
	return selection;
}