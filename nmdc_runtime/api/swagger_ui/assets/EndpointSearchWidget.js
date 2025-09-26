/**
 * Endpoint search widget, implemented as a Web Component.
 * 
 * The widget provides a search input field, and a results panel that lists
 * the API endpoints whose URL paths match the search term. When the user
 * clicks on a search result, the browser navigates to the corresponding
 * endpoint in the Swagger UI page (via a full page refresh/"deep link").
 *
 * References:
 * - https://developer.mozilla.org/en-US/docs/Web/API/Web_components
 * - https://open-wc.org/codelabs/basics/web-components#2
 */
class EndpointSearchWidget extends HTMLElement {
    constructor() {
        super();
    }

    /**
     * Initializes the custom HTML element after it's been added to the DOM.
     * 
     * Note: The `connectedCallback` function gets called automatically when
     *       this HTML element gets added to the DOM.
     */
    connectedCallback() {
        // Create a Shadow DOM tree specific to this custom HTML element, so that its styles don't
        // impact other elements on the page and so styles on the page don't impact this element.
        // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_shadow_DOM#creating_a_shadow_dom
        this.attachShadow({ mode: "open" });

        // Implement the widget.
        const containerEl = document.createElement("div");
        const innerContainerEl = document.createElement("div");
        const inputEl = document.createElement("input");
        this.resultsPanelEl = document.createElement("div");
        this.resultsListEl = document.createElement("ul");
        containerEl.classList = "container";
        innerContainerEl.classList = "inner-container";
        this.resultsPanelEl.classList = "results-panel";
        inputEl.name = "search-term";
        inputEl.placeholder = "Find an endpoint...";
        this.resultsPanelEl.appendChild(this.resultsListEl);
        innerContainerEl.appendChild(inputEl);
        innerContainerEl.appendChild(this.resultsPanelEl);
        containerEl.appendChild(innerContainerEl);

        // Make an array of all the endpoints that Swagger UI knows about. This will be our search index.
        //
        // Note: In an earlier implementation of this step, we queried the DOM for this information.
        //       However, that didn't work when any of the endpoint groups were collapsed. So, instead,
        //       we access the data structure returned by the `SwaggerUI()` constructor, which is
        //       invoked by the JavaScript code built into FastAPI (that JavaScript code assigns
        //       the return value to a global variable named `ui`, which we access here).
        //
        // TODO: Consider extracting the endpoint's description, too, so we can display a portion of it
        //       in the search results. It can be obtained via `opMap.get("operation").get("description")`.
        //
        const operationMaps = Array.from(ui.specSelectors.operations());
        this.endpoints = operationMaps.map(opMap => ({
            urlPath: opMap.get("path"), // e.g. "/studies/{study_id}"
            httpMethod: opMap.get("method").toUpperCase(), // e.g. "GET"
            operationId: opMap.get("operation").get("operationId"), // e.g. "find_studies_studies_get"
            tag: opMap.get("operation").get("tags").get(0), // e.g. "Metadata access: Find"
        }));
        console.debug(`Found ${this.endpoints.length} endpoints in OpenAPI schema`);

        // Make it so the search results update whenever the value of the search input
        // changes as the result of a user action (e.g. typing, cutting, pasting).
        // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Element/input_event
        inputEl.addEventListener("input", (event) => {
            this.updateSearchResults(event.target.value);
        });

        // Mount the container element to the custom element's Shadow DOM tree.
        this.shadowRoot.appendChild(containerEl);

        // Add styles to the custom element's Shadow DOM tree.
        // Note: We try to mimic the appearance of native Swagger UI elements.
        const styleEl = document.createElement("style");
        styleEl.textContent = `
            .container {
                margin: 0 auto;
                max-width: 1460px;
                font-family: sans-serif;
                font-size: 14px;
                color: #3b4151;
            }
            .inner-container {
                margin: 20px 20px 0px 20px;
            }
            input {
                padding: 8px 10px;
                box-sizing: border-box;
                border: 1px solid #d9d9d9;
                border-radius: 4px;
                width: 100%;
            }
            .results-panel {
                background-color: rgba(0, 0, 0, .05);
                border-radius: 4px;
            }
            .results-panel .message {
                padding: 26px 26px 0px 26px;
            }
            ul {
                list-style-type: none;
                padding-left: 26px;
                padding-right: 26px;
                font-family: monospace;
            }
            ul > li:first-child {
                padding-top: 26px;
            }
            ul > li:last-child {
                padding-bottom: 26px;
            }
            li > a {
                color: #4990e2;
                text-decoration: none;
            }
            li > a:hover {
                color: #1f69c0;
            }
            li > a .http-method {
                display: inline-block;
                min-width: 52px;
                margin-right: 10px;
            }
            .matching-substring {
                background-color: #f9f871;
            }
        `;
        this.shadowRoot.appendChild(styleEl);

        // Initialize the search results to be empty.
        this.updateSearchResults("");
    }

    updateSearchResults(searchTerm) {
        // Special case: If the search term is empty, clear the search results.
        if (searchTerm.trim().length === 0) {
            this.resultsListEl.replaceChildren();
            return;
        }

        // Identify the matching endpoints (case-insensitively).
        const lowercaseSearchTerm = searchTerm.toLowerCase();
        const matchingEndpoints = this.endpoints.filter(item => {
            const lowercaseUrlPath = item.urlPath.toLowerCase();
            return lowercaseUrlPath.includes(lowercaseSearchTerm);
        });
        
        // If there are no matching endpoints, clear the search results.
        // TODO: Display a message saying there are no matching endpoints.
        if (matchingEndpoints.length === 0) {
            this.resultsListEl.replaceChildren();
            return;
        }

        // Update the search results to list the matching endpoints.
        const resultEls = matchingEndpoints.sort((a, b) => {
            // If the URL paths are identical, sort by HTTP method; otherwise, sort by URL path.
            // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/localeCompare
            if (a.urlPath.localeCompare(b.urlPath) === 0) {
                return a.httpMethod.localeCompare(b.httpMethod);
            } else {
                return a.urlPath.localeCompare(b.urlPath);
            }
        }).map(matchingEndpoint => {
            const liEl = document.createElement("li");
            const aEl = document.createElement("a");

            // Here, we distinguish the part of the URL path that matched the search term.
            const urlPathSpanEl = document.createElement("span");
            const lowercaseUrlPath = matchingEndpoint.urlPath.toLowerCase();
            const substrCharIdx = lowercaseUrlPath.indexOf(lowercaseSearchTerm);
            if (substrCharIdx === -1) {
                // Note: I don't expect this to ever happen, since we already know this
                //       endpoint matches the search term.
                urlPathSpanEl.textContent = matchingEndpoint.urlPath;
            } else {
                const substrSpanEl = document.createElement("span");
                const preSubstrChars = matchingEndpoint.urlPath.substring(0, substrCharIdx);
                const substrChars = matchingEndpoint.urlPath.substring(substrCharIdx, substrCharIdx + searchTerm.length);
                const postSubstrChars = matchingEndpoint.urlPath.substring(substrCharIdx + searchTerm.length);
                substrSpanEl.classList = "matching-substring";
                substrSpanEl.textContent = substrChars;
                urlPathSpanEl.append(preSubstrChars, substrSpanEl, postSubstrChars);
            }

            // Here, we build a "deep link" to the corresponding endpoint, using the syntax
            // shown in the Swagger UI "Deep Linking" documentation, at:
            // https://swagger.io/docs/open-source-tools/swagger-ui/usage/deep-linking/#usage
            //
            // Note: The reason we don't use something like `scrollTo` or `scrollIntoView`
            //       is that the target element may not be mounted to the DOM right now,
            //       due to it being within a collapsed section. Instead, we send the
            //       browser to the deep link URL (Swagger UI will automatically expand
            //       the relevant section when the page loads at that deep link URL).
            //
            const urlWithoutQueryStr = window.location.origin + window.location.pathname;
            const tagPart = encodeURIComponent(matchingEndpoint.tag);
            const operationPart = encodeURIComponent(matchingEndpoint.operationId);
            const httpMethodSpanEl = document.createElement("span");
            httpMethodSpanEl.classList = "http-method";  // e.g. "<span class="http-method">GET</span>"
            httpMethodSpanEl.textContent = matchingEndpoint.httpMethod;
            aEl.append(httpMethodSpanEl, urlPathSpanEl);
            aEl.href = `${urlWithoutQueryStr}/#${tagPart}/${operationPart}`;
            liEl.appendChild(aEl);
            return liEl;
        });
        this.resultsListEl.replaceChildren(...resultEls);
    }
}

// Register a custom HTML element (`<endpoint-search-widget>`).
// Docs: https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements#registering_a_custom_element
customElements.define("endpoint-search-widget", EndpointSearchWidget);
