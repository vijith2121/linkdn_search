import scrapy
from linkdn_search.items import LinkdnItem
import os
import pandas as pd
from curl_cffi import requests
from lxml import html
import json
from datetime import timedelta
from datetime import date

class Linkdn_searchSpider(scrapy.Spider):
    name = "linkdn_search"
    start_urls = ["https://example.com"]

    def parse(self, response):

        cookies = {
            'li_at': 'AQEDAQcfXvwCzdXsAAABlbNMwkwAAAGV11lGTFYATwgBEw5Agx9JHrjBhsFcAccRQaovN3ziHErePvakEl0kl_97m0RCaO7OFVCcj4sQI2BehT9UX9BsJ9wOAknc5GtpLItqf6McT6K11otK64yde0ai',
            'JSESSIONID': '"ajax:6973308358734566436"',
        }

        headers = {
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'accept-language': 'en-US,en;q=0.9',
            'csrf-token': 'ajax:6973308358734566436',
            'priority': 'u=1, i',
            'referer': 'https://www.linkedin.com/search/results/all/?keywords=tarek%20said%20fathy%20arafa&origin=TYPEAHEAD_ESCAPE_HATCH&sid=w2Q',
            'sec-ch-ua': '"Not:A-Brand";v="24", "Chromium";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'x-li-lang': 'en_US',
            'x-li-page-instance': 'urn:li:page:d_flagship3_search_srp_people;WvHTLUuzTbSiQTwRP4ODvQ==',
            'x-li-pem-metadata': 'Voyager - People SRP=search-results',
            'x-li-track': '{"clientVersion":"1.13.32603","mpVersion":"1.13.32603","osName":"web","timezoneOffset":5.5,"timezone":"Asia/Calcutta","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":1366,"displayHeight":768}',
            'x-restli-protocol-version': '2.0.0',
            # 'cookie': 'lang=v=2&lang=en-us; bcookie="v=2&e572265c-9844-44cf-8f43-baf2ee5fddd2"; bscookie="v=1&202503201123315eb9fee1-47ee-46e7-88f1-c0a5e28a1390AQH3Hqps0gzOw7knxFwXo4x6ZT50Y9TF"; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; aam_uuid=84403664962476641350117828440227962376; _gcl_au=1.1.850225740.1742469850; li_rm=AQEFb9Fin6zU8gAAAZWzTDPrvQyoMgFt4yemS9YcwN7X6GWZSv1qCJ29bXsxwQx8j7Sy1dMDElpEq9JMroNNrCyWi1YDhLzHvqZoYB7t-znxgz-UGuWv0GAY; g_state={"i_l":0}; liap=true; li_at=AQEDAQcfXvwCzdXsAAABlbNMwkwAAAGV11lGTFYATwgBEw5Agx9JHrjBhsFcAccRQaovN3ziHErePvakEl0kl_97m0RCaO7OFVCcj4sQI2BehT9UX9BsJ9wOAknc5GtpLItqf6McT6K11otK64yde0ai; JSESSIONID="ajax:6973308358734566436"; timezone=Asia/Calcutta; li_theme=light; li_theme_set=app; _guid=bb7260de-ec94-4b57-831d-165546ad96f3; li_sugr=004db910-fc39-441d-991e-1bd90daa53fa; UserMatchHistory=AQI8l4KtOQuCYQAAAZWzTNhBAQ5cF0FXmwm33iFtB5Vu6YmYEf_jEtUMjDz-wlbHv3ookeTi40ulCJfFD2vHhAqV61963SihsudMzlLGpmGsXaog2lyV-y4x7VQ0vvU7higZnJewbD0ckaE76O1wIuWt4SAm7S-X4KouyZHQ-XOzTzGy-5HfxYH5fbdKTr3BZVq3Tte1kGvWU_qqhKQIKRY24XVI_ueZ2JxtKywMezuR0i2T_WsK1_4qu_FeZIgOTesDosk0Dnb_0xuPpzy02DKI2tSV2V2io4yQ9PZuxNn8dMaGBKw0C_ZYeNghF0Wn1HKO0Ev2AYRglfR-XOnyEJSYMtRP9j3uzpKj8ak3H_d6EIcNEA; AnalyticsSyncHistory=AQL28SrUbBzcPwAAAZWzTNhBW4S1C-asPbPs6yC8at3EyaFDjHdhSDrq5xMb1JfVt3dj3vzTvKHtDm5jQAQkDw; lms_ads=AQHaf1d5z_XMowAAAZWzTNlA6YxTZoRZ0oA_L9PfArXexNbd6Y7EASPaugRBc8syVvLjhUiV74setPMkYoISxS8_SUFkGGAW; lms_analytics=AQHaf1d5z_XMowAAAZWzTNlA6YxTZoRZ0oA_L9PfArXexNbd6Y7EASPaugRBc8syVvLjhUiV74setPMkYoISxS8_SUFkGGAW; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C20168%7CMCMID%7C84974784770988927480102935639326666179%7CMCAAMLH-1743074713%7C12%7CMCAAMB-1743074713%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1742477113s%7CNONE%7CvVersion%7C5.1.1%7CMCCIDH%7C282707208; dfpfpt=dca0eb8b8b3941f3832ad84de974cd78; fptctx2=taBcrIH61PuCVH7eNCyH0LNKRXFdWqLJ6b8ywJyet7UuqrNYJV9iF5%252bFx4LJa4tZi%252fYkSOXcBLoOTjCJbiBBDXoNCYhU7T5Tb0Vj6dgDV2IS6SgfNRpX23BJT1UbUEq8NtXnLYFLP8hvc%252fIBpWUQwg4%252ba18aCCbyHgxUthNzRAKpN6ZWWEv5fLmeqRadblvswV7YnPI6soXQ3vEVMYzQFRKQuy6qEeBnKCpjz%252bYV%252bAo12puq25UHASSBxfYv7JieuGUP9VCP46EP1R%252fbv5Hglgfg65K2SE7CtZLvYDypwVpfuuBVxCcmEh5CnMJ3Jg8A29HUCy2P%252fM2iSP52Zf9TAKXrH6NBCZTD%252fURpjfL9x0s%253d; lidc="b=OB44:s=O:r=O:a=O:p=O:g=3855:u=682:x=1:i=1742469913:t=1742547767:v=2:sig=AQGxZIwHEQh3FBKNgRL9yYble1kV95Ku"',
        }
        # name = 'HATEM ABDELWAHAB ELSAYED AWADALLA'
        script_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_directory, "MOL_UAE_DATA2.csv")
        df = pd.read_csv(file_path)
        for item in df.to_dict('records'):
            search_name = item.get('nameen', '')
            # search_name = 'HATEM ABDELWAHAB ELSAYED AWADALLA'
            headers['referer'] = f'https://www.linkedin.com/search/results/all/?keywords={search_name}&origin=TYPEAHEAD_ESCAPE_HATCH&sid=w2Q'
            yield scrapy.Request(
                f'https://www.linkedin.com/voyager/api/graphql?variables=(start:0,origin:SWITCH_SEARCH_VERTICAL,query:(keywords:{search_name},flagshipSearchIntent:SEARCH_SRP,queryParameters:List((key:resultType,value:List(PEOPLE))),includeFiltersInResponse:false))&queryId=voyagerSearchDashClusters.9c3177ca40ed191b452e1074f52445a8',
                cookies=cookies,
                headers=headers,
                callback=self.parse_profile
            )
            # return

    def parse_profile(self, response):
        profile_urls = response.json().get('included', [])
        # urls_list = []
        # print([i.get('navigationUrl') for i in profile_urls if i.get('navigationUrl') != None])
        # for i in profile_urls:
        #     urls_list.append(profile_url.get('navigationUrl')) 
        for profile_url in profile_urls:
            # print(profile_url.get('navigationUrl'))
            if profile_url.get('navigationUrl') != None:
                profile_pictures_url = profile_url.get(
                    'image', {}
                    ).get(
                        'attributes', []
                        )[0].get(
                            'detailData', {}
                            ).get(
                                'nonEntityProfilePicture', {}
                                ).get(
                                    'vectorImage', {}
                                    ).get(
                                        'artifacts', []
                                        )[0].get(
                                            'fileIdentifyingUrlPathSegment', ''
                                            )
                url = profile_url.get('navigationUrl')
                profile_url_slug = url.split('/')[-1].split('?')[0]
                # return
                cookies = {
                            'li_at': 'AQEDAQcfXvwDkE1IAAABlJwBaOsAAAGUwA3s61YAf549Kpn8iRXD0GiabxO3-BO4xrCo3gNLSgxWC7ARu0ispFEqPB_iFROte6hu3n9Z-EQXPr_MysYMqm1jBpW8Aj2yWXx3_0tjCHvczweupw2wYFPK',
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
                    'cache-control': 'no-cache',
                    'pragma': 'no-cache',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                }
                meta = {
                    'profile_url': url,
                    'profile_url_slug': profile_url_slug,
                    'cookies': cookies,
                    'headers': headers,
                    'profile_pictures_url': profile_pictures_url
                }
                yield scrapy.Request(
                    url=url, callback=self.parse_detail_page, cookies=cookies, headers=headers, meta=meta
                    )

    def parse_detail_page(self, response):
        meta_data = response.meta
        parser = html.fromstring(response.text)
        # print(parser.xpath("//span[text()= 'About']//parent::h2//parent::div//parent::div//parent::div//parent::div//following-sibling::div//span//text()"))
        profile_slug = meta_data.get('profile_url_slug')
        profile_xpath = f"//code[contains(@id, 'bpr-guid')][contains(text(), '{profile_slug}')]//text()"
        profile_items = parser.xpath(profile_xpath)
        profile_data = json.loads(
            "".join(profile_items[0]).strip().replace(' ', '').replace('//', '').replace('\\u003D', '').strip()).get('included', []
        )
        for item in profile_data:
            publicIdentifier = item.get('publicIdentifier')
            if publicIdentifier and publicIdentifier in response.url:
                headline = item.get('headline', None)
                first_name = item.get('firstName', None)
                last_name = item.get('lastName', None)
                about_api_key = item.get("entityUrn")
                meta_data['about_api_key'] = about_api_key
                meta_data['first_name'] = first_name
                meta_data['headline'] = headline
                meta_data['last_name'] = last_name
                meta_data['publicIdentifier'] = publicIdentifier
                show_more_profile_urls = None

                if item.get('entityUrn'):
                    show_more_profile_urls = (
                        f'https://www.linkedin.com/in/{profile_slug}/overlay/browsemap-recommendations/?isPrefetched=true&profileUrn={item.get("entityUrn")}'
                    )
                    meta_data['show_more_profile_urls'] = show_more_profile_urls
                    yield scrapy.Request(
                        url=show_more_profile_urls, callback=self.show_more_profile, cookies=meta_data.get('cookies'), headers=meta_data.get('headers'), meta=meta_data
                    )

    def show_more_profile(self, response):
        cookies = {
            'lang': 'v=2&lang=en-us',
            'li_at': 'AQEDAQcfXvwFoVp2AAABlK3xkiUAAAGU0f4WJU4AN4FLfebPE72-XDSXKgeSAvAsstHcR-1JrQZyGmTnX5nIsruFNVO-ywWERZe89DusZDlx3AbCuf2l30yFlRCwouPGBGV9Zhxg6LN5HKggV2463U75',
            'JSESSIONID': '"ajax:7930250182375445394"'
        }

        headers = {
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'cache-control': 'no-cache',
            'csrf-token': 'ajax:7930250182375445394',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.linkedin.com/in/mishel-john-56223156/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-li-lang': 'en_US',
            'x-li-page-instance': 'urn:li:page:d_flagship3_profile_view_base;oBmXUY8DQ1GqfL4Sswt+cA==',
            'x-li-pem-metadata': 'Voyager - Profile=profile-tab-initial-cards',
            'x-li-track': '{"clientVersion":"1.13.29542","mpVersion":"1.13.29542","osName":"web","timezoneOffset":5.5,"timezone":"Asia/Calcutta","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":1920,"displayHeight":1080}',
            'x-restli-protocol-version': '2.0.0',
        }
        parser = html.fromstring(response.text)
        meta_data = response.meta
        show_more_profile_data_xpath = "//code[contains(text(), 'More profiles for you')]//text()"
        show_more_profile_data = parser.xpath(show_more_profile_data_xpath)

        show_more_profile_items = json.loads("".join(show_more_profile_data)).get('data', {}).get('data', {}).get('identityDashProfileComponentsBySectionType', {}).get('elements', [])
        show_more_profile_get = show_more_profile_get_items(show_more_profile_items)

        encoded_url = meta_data.get("about_api_key").replace(':', '%3A')
        about_api = (
            f'https://www.linkedin.com/voyager/api/graphql?variables=(profileUrn:{encoded_url})&queryId=voyagerIdentityDashProfileCards.477a15aef40d846236e0ad896b84f7e0'
        )
        # about_api = 'https://www.linkedin.com/voyager/api/graphql?includeWebMetadata=true&variables=(profileUrn:urn%3Ali%3Afsd_profile%3AACoAAAvHtNoBEyGzBjx0xD1rn4-BMkRRnkMStoo)&queryId=voyagerIdentityDashProfileCards.e832334bfcc658dc78dcdc9492d65ea1'
        headers['referer'] = f'{meta_data.get("profile_url")}'
        res = requests.get(about_api, headers=headers, cookies=cookies)
        for item in res.json().get('included', []):
            if item.get('topComponents'):
                for item_data in item.get('topComponents'):
                    try:
                        causes = item_data.get('components', {}).get('headerComponent', None).get('title', None).get('text')
                    except Exception as error:
                        causes = None
                    if causes and causes == 'About':
                        about = get_about(item.get('topComponents', []))
                        meta_data['about'] = about
                    if causes and causes == 'Causes':
                        Causes = get_about(item.get('topComponents', []))
                        meta_data['Causes'] = Causes

        scrape_date = date.today()
        data = {
            'profile_url': meta_data.get('profile_url', None),
            'profile_url_slug': meta_data.get('profile_url_slug', None),
            'first_name': meta_data.get('first_name', None),
            'last_name': meta_data.get('last_name', None),
            'headline': meta_data.get('headline', None),
            'publicIdentifier': meta_data.get('publicIdentifier', None),
            'show_more_profile_urls': meta_data.get('show_more_profile_urls', None),
            'about': meta_data.get('about', None),
            'Causes': meta_data.get('Causes', None),
            'show_more_profile': show_more_profile_get,
            'scrape_date': str(scrape_date),
            'profile_pictures_url': meta_data.get('profile_pictures_url', None)
        }
        yield LinkdnItem(**data)

def get_about(data):
    about_list = []
    for item in data:
        if item.get('components', {}).get('textComponent'):
            about_list.append(
                item.get('components', {}).get('textComponent').get('text').get('text').strip()
            )
    return "".join(about_list).strip() if about_list else None

def show_more_profile_get_items(show_more_profile_items):
    item_lists = []
    for item in show_more_profile_items:
        item_profile = item.get('components', {}).get('fixedListComponent', {}).get('components', [])
        for get_profile in item_profile:
            profile_items = get_profile.get('components', {}).get('entityComponent', {})
            name = profile_items.get('titleV2', {}).get('text', {}).get('text', '')
            subtitle = profile_items.get('subtitle', {}).get('text', '')
            profile_url = profile_items.get('textActionTarget', '')
            datas = {
                'name': name,
                'subtitle': subtitle,
                'profile_url': profile_url
            }
            item_lists.append(datas)
    return item_lists if item_lists else []
