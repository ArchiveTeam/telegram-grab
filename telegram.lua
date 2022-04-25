dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_channel = nil
local item_post = nil

local selftext = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local queue_resources = true

local discovered_outlinks = {}
local discovered_items = {}
local discovered_channels = {}
local bad_items = {}
local ids = {}
local allowed_resources = {}

local retry_url = false

local current_js = {
  ["widget-frame.js"] = "56",
  ["tgwallpaper.min.js"] = "3",
  ["tgsticker.js"] = "27",
  ["telegram-web.js"] = "14",
  ["telegram-widget.js"] = "19",
  ["discussion-widget.js"] = "9"
}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    target[item] = true
  end
end

find_item = function(url)
  local value = nil
  local type_ = nil
  --[[if not string.match(url, "^https?://t%.me/")
    and not string.match(url, "^https?://www%.t%.me/")
    and not string.match(url, "^https?://telegram%.me/")
    and not string.match(url, "^https?://www%.telegram%.me/") then
    value = url
    type_ = 'url'
  end]]
  if not value then
    value = string.match(url, "^https?://t%.me/s/([^/%?&]+)$")
    type_ = 'channel'
  end
  if not value then
    value = string.match(url, "^https?://t%.me/([^/]+/[^/]+)%?embed=1&single=1$")
    type_ = 'post'
  end
  if value then
    item_type = type_
    if --[[type_ == "url" or]] type_ == "channel" then
      item_value = value
      if type_ == "channel" then
        item_channel = value
        ids[value] = true
      end
    elseif type_ == "post" then
      item_value = string.gsub(value, "/", ":")
      item_channel, item_post = string.match(value, "^([^/]+)/(.+)$")
      ids[item_post] = true
    end
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      abortgrab = false
      queue_resources = true
      retry_url = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if string.match(url, "%?q=") then
    return false
  end

  for _, pattern in pairs({
    "^https?://[^/]+%.me/([^/%?&#]+)",
    "^https?://[^/]+%.me/s/([^/%?&#]+)"
  }) do
    local new_channel = string.match(url, pattern)
    if new_channel
      and new_channel ~= "s"
      and new_channel ~= "api"
      and new_channel ~= "css" then
      discover_item(discovered_channels, "channel:" .. new_channel)
    end
  end

  if string.match(url, "^https?://[^/]*telesco%.pe/")
    or string.match(url, "^https?://[^/]*telegram%-cdn%.org/") then
    if item_type == "url" then
      return true
    end
    if allowed_resources[url] then
      return true
    end
    if not queue_resources then
      return false
    end
    allowed_resources[url] = true
    return allowed(url, parenturl)
  end

  if not string.match(url, "^https?://t%.me/")
    and not string.match(url, "^https?://www%.t%.me/")
    and not string.match(url, "^https?://telegram%.me/")
    and not string.match(url, "^https?://www%.telegram%.me/") then
    local temp = ""
    for c in string.gmatch(url, "(.)") do
      local b = string.byte(c)
      if b < 32 or b > 126 then
        c = string.format("%%%02X", b)
      end
      temp = temp .. c
    end
    discover_item(discovered_outlinks, string.match(temp, "^([^%s]+)"))
    return false
  end

  if item_type == "post" then
    for s in string.gmatch(url, "([0-9a-zA-Z]+)") do
      if ids[s] then
        return true
      end
    end
  end

  if item_type == "channel" then
    if string.match(url, "^https?://[^/]+/[^/]+/[0-9]+")
      or string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+")
      or string.match(url, "%?before=")
      or string.match(url, "%?after=")
      or string.match(url, "%?q=") then
      return false
    end
    for s in string.gmatch(url, "([^/%?&]+)") do
      if ids[s] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if item_type == "url" then
    return false
  end

  if not processed(url) and allowed(url, parent["url"])
    and string.match(url, "^https?://[^/]+%.me/") then
    addedtolist[url] = true
    return true
  end]]
  
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  if item_type == "url" then
    return urls
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and string.match(url_, "^https?://[^/%.]+%..+")
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  queue_resources = true

  local domain, path = string.match(url, "^https?://([^/]+)(/.+)$")
  if (
    domain == "www.t.me"
    or domain == "t.me"
    or domain == "www.telegram.me"
    or domain == "telegram.me"
  ) and not string.match(url, "%?embed=1") then
    check("https://t.me" .. path)
    check("https://telegram.me" .. path)
  elseif string.match(domain, "telesco%.pe") then
    check(string.gsub(url, "telesco%.pe", "telegram%-cdn%.org"))
  elseif string.match(domain, "telegram%-cdn%.org") then
    check(string.gsub(url, "telegram%-cdn%.org", "telesco%.pe"))
  end

  if allowed(url) and status_code < 300
    and string.match(url, "^https?://[^/]+%.me/") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]+/[^/]+/[0-9]+%?embed=1&single=1$") then
      --[[local html_new = string.gsub(html, '<div%s+class="tgme_widget_message_user">.-</div>', "") 
      if html == html_new then
        io.stdout:write("No profile image.\n")
        io.stdout:flush()
        abort_item()
        return {}
      end
      html = html_new]]
      local base = string.match(url, "^([^%?]+)")
      check(base .. "?embed=1&discussion=1")
      --check(base .. "?embed=1&discussion=1&comments_limit=5")
      check(base)
      check(base .. "?single")
      check(base .. "?single=1")
      check(base .. "?embed=1")
      check(base .. "?embed=1&mode=tme")
      check(base .. "?embed=1&single=1")
      check(base .. "?embed=1&mode=tme&single=1")
      check(string.gsub(url, "^(https?://[^/]+/)([^%?]+)%?.*", "%1s/%2"))
      check(string.gsub(url, "^(https?://[^/]+/)([^%?]-)/([0-9]+)%?.*", "%1s/%2?before=%3"))
      check(string.gsub(url, "^(https?://[^/]+/)([^%?]-)/([0-9]+)%?.*", "%1s/%2?after=%3"))
    elseif --[[string.match(url, "^https?://[^/]+/[^/]+/[0-9]+")
      or]] string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+") then
      queue_resources = false
    end
    if string.match(url, "^https?://[^/]+/s/[^/%?&]+$") then
      check(string.gsub(url, "^(https?://[^/]+/)s/([^/%?&]+)$", "%1%2"))
      local highest_id = -1
      for id in string.gmatch(html, 'data%-post="' .. item_channel .. '/([0-9]+)"') do
        id = tonumber(id)
        if id > highest_id then
          highest_id = id
        end
      end
      if highest_id > -1 then
        for i=0,highest_id do
          discover_item(discovered_items, "post:" .. item_channel .. ":" .. tostring(i))
        end
      end
      local image_url = string.match(html, '<meta%s+property="og:image"%s+content="([^"]+)"')
      local twitter_url = string.match(html, '<meta%s+property="twitter:image"%s+content="([^"]+)"')
      if image_url ~= twitter_url then
        io.stdout:write("Profile images not equal for og:image and twitter:image.\n")
        io.stdout:flush()
        abort_item()
        return {}
      end
      check(image_url)
      for newurl in string.gmatch(html, '<i%s+class="tgme_page_photo_image[^"]+"[^>]+>%s*<img%s+src="([^"]+)"') do
        check(newurl)
      end
    end
    if item_type == "post"
      and (
        string.match(url, "^https?://[^/]+/s/[^/%?&]+%?")
        or string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+")
      ) then
      queue_resources = false
    end
    html = string.gsub(html, "</span>", "")
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])

  if abortgrab then
    abort_item()
    return false
  end

  if http_stat["statcode"] ~= 200 and not string.match(url["url"], "%?single") then
    io.stdout:write("Status code not 200\n")
    io.stdout:flush()
    retry_url = true
    return false
  end

  if string.match(url["url"], "^https?://[^/]+%.me/") then
    local html = read_file(http_stat["local_file"])
    for js_name, version in string.gmatch(html, "([^/]+%.js)%?([0-9]+)") do
      if current_js[js_name] ~= version then
        io.stdout:write("Script " .. js_name .. " with version " .. version .. " is not known.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
    end
    if string.match(url["url"], "%?embed=1&discussion=1") then
      if string.match(html, '"comments_cnt"') then
        io.stdout:write("Found discussions comments. Not currently supported.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
      return true
    end
    if not string.match(html, "telegram%-cdn%.org")
      and not string.match(html, "telesco%.pe") then
      io.stdout:write("Could not find CDNs.\n")
      io.stdout:flush()
      retry_url = true
      return false
    end
    if string.match(url["url"], "[%?&]embed=1") then
      if string.match(html, "tgme_widget_message_error")
        or not string.match(html, "tgme_widget_message_author") then
        io.stdout:write("Post does not exist.\n")
        io.stdout:flush()
        retry_url = true
        return false
      end
    elseif http_stat["statcode"] == 200 then
      local image_domain = string.match(html, '<meta%s+property="og:image"%s+content="https?://([^/"]+)')
      if not image_domain or (
        not string.match(image_domain, "telegram%-cdn%.org")
        and not string.match(image_domain, "telesco%.pe")
      ) then
        io.stdout:write("Main image has bad domain.\n")
        io.stdout:flush()
        retry_url = true
        return false
      end
    end
  end

  retry_url = false
  tries = 0

  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if retry_url or status_code == 0 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 12
    if (item_type == "post" and string.match(url["url"], "%?embed=1&single=1$"))
      or (item_type == "channel" and string.match(url["url"], "^https?://t%.me/s/([^/%?&]+)$")) then
      io.stdout:write("Bad response on first URL.\n")
      io.stdout:flush()
      maxtries = 0
    end
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 4
    while tries < maxtries do
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      abortgrab = true
    end
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["telegram-x2kj4uadm0lrniv"] = discovered_items,
    ["telegram-iy46ve7bql0k79p"] = discovered_channels,
    ["telegram-channels-aqpadsraxi2b78y"] = discovered_channels,
    ["urls-h051713fi1agegy"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
  end
  return exit_status
end

