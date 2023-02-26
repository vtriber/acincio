import asyncio
import aiohttp
import datetime

from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople

CHUNK_SIZE = 10


async def get_star_wars(session, url):
    async with session.get(url) as response:
        json_data = await response.json()
        return json_data


async def paste_to_db(results_list):
    for results in results_list:
        swapi_people = [SwapiPeople(**result) for result in results]
        async with Session() as session:
            session.add_all(swapi_people)
            await session.commit()


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    coros_piple = (get_star_wars(session, f'https://swapi.dev/api/people/{i}') for i in range(1, 83) if i != 17)
    results_list = []
    for coros_chunk in chunked(coros_piple, CHUNK_SIZE):
        results = await asyncio.gather(*coros_chunk)
        results_list.append(results)

    for results in results_list:
        for result in results:
            result['id'] = int(result['url'][-3:].strip('/'))
            result.pop('url')
            result.pop('created')
            result.pop('edited')

            film_list = []
            for film in result['films']:
                film_json = await get_star_wars(session, film)
                film_list.append(film_json['title'])
            result['films'] = ', '.join(film_list)

            species_list = []
            for specie in result['species']:
                if specie != []:
                    specie_json = await get_star_wars(session, specie)
                    species_list.append(specie_json['name'])
            result['species'] = ', '.join(species_list)

            starship_list = []
            for starship in result['starships']:
                if starship != []:
                    starship_json = await get_star_wars(session, starship)
                    starship_list.append(starship_json['name'])
            result['starships'] = ', '.join(starship_list)

            vehicle_list = []
            for vehicle in result['vehicles']:
                if vehicle != []:
                    vehicle_json = await get_star_wars(session, vehicle)
                    vehicle_list.append(vehicle_json['name'])
            result['vehicles'] = ', '.join(vehicle_list)

    asyncio.create_task(paste_to_db(results_list))

    await session.close()
    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task


if __name__ == '__main__':
    asyncio.run(main())