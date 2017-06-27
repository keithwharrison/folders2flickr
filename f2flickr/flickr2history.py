#!/usr/bin/python
"""
A way to recreate the history database of uploaded files from the photos stored
on flickr.
"""

__author__ = "pkolarov@gmail.com"

import logging
import shelve
import sys
import os
import f2flickr.flickr as flickr
from pprint import pprint

def getPhotoIDbyTag(tag, user):
    """
    Get one and only one photo for the given tags or None
    this works only if we previously tagged all the pics on Flickr with
    uploader tool automaticaly

    Plus delete images that contain the same TAGS !!!!
    """
    retries = 0
    photos = None
    while (retries < 3):
        try:
            logging.debug(user.id)
            photos = flickr.photos_search(user_id=user.id, auth=all, tags=tag,
                                          tag_mode='any')
            break
        except:
            logging.error("flickr2history: Flickr error in search, retrying")
            logging.error(sys.exc_info()[0])

        retries = retries + 1

    if (not photos or len(photos) == 0):
        logging.debug("flickr2history: No image in Flickr (yet) with tags %s " +
                      "(possibly deleted in Flickr by user)", tag)
        return None

    logging.debug("flickr2history: Tag=%s found %d", tag, len(photos))
    while (len(photos)>1):
        logging.debug("flickr2history: Tag %s matches %d images!",
                      tag, len(photos))
        logging.debug("flickr2history: Removing other images")
        try:
            photos.pop().delete()
        except:
            logging.error("flickr2history: Flickr error while " +
                          "deleting duplicate image")
            logging.error(sys.exc_info()[0])

    return photos[0]

def convert_format(images, imageDir, historyFile):
    """
    Convert a history file from old format to new that allows updated local files
    to be synced.
    For each file, store the following information:
    - Photo ID from Flickr
    - Modification time
    - Size of file
    """
    logging.debug('flickr2history: Started convert_format')

    uploaded = shelve.open( historyFile )
    num_images=len(images)
    num_ok=0
    num_converted=0
    num_not_found=0
    for i,image in enumerate(images):
        if (i+1) % 1000 == 0:
            sys.stdout.write('.'); sys.stdout.flush()
        full_image_path=image
        # remove absolute directory
        image = str(image[len(imageDir):])
        if uploaded.has_key(image):
            if isinstance(uploaded[image], tuple):
                num_ok += 1
                continue
        logging.debug("Converting history data for photo %s", image)
        try:
            photo_id=uploaded[image]
        except KeyError:
            logging.debug('Photo %s cannot be found from history file' % image)
            num_not_found += 1
            continue
        try:
            stats = os.stat(full_image_path)
            file_mtime=stats.st_mtime
            file_size=stats.st_size
        except OSError:
            file_mtime = 0
            file_size = 0
        uploaded[ image] = ( photo_id, file_mtime, file_size )
        uploaded[ photo_id ] = image
        num_converted += 1
    sys.stdout.write('\n'); sys.stdout.flush()
    logging.info('num_images=%d num_ok=%d num_not_found=%d num_converted=%d' %
                     (num_images, num_ok, num_not_found, num_converted))
    uploaded.close()

def get_photos_from_flickr():
    """
    Get all photo ids from flickr
    """
    logging.debug('flickr2history: get_photo_ids_from_flickr')
    try:
        user = flickr.test_login()
        logging.debug(user.id)
    except:
        logging.error(sys.exc_info()[0])
        return None

    per_page = 500

    logging.debug("Fetching page 1...")    
    photos, pages = flickr.photos_search_with_pages(user_id=user.id, auth=all, per_page=per_page);
    photodict = {}
    for photo in photos:
        photodict[photo.id] = photo

    for page in range(2, pages + 1):
        logging.debug("Fetching page {}...".format(page))    
        photos, pages = flickr.photos_search_with_pages(user_id=user.id, auth=all, per_page=per_page, page=page);
        for photo in photos:
            photodict[photo.id] = photo

    return photodict

def get_photo_ids_from_database_file(history_file):
    """
    Get all photo ids from the history database file
    """
    history = shelve.open(history_file)
    return get_photo_ids_from_database(history)

def get_photo_ids_from_database(history):
    """
    Get all photo ids from the history database
    """
    return list(filter(lambda x: not x.startswith('/'), history.keys()))

def get_photo_paths_from_database(history):
    """
    Get all photo paths from the history database
    """
    return list(filter(lambda x: x.startswith('/'), history.keys()))

def database_compare(images, image_dir, history_file, absolute_path):
    history = shelve.open(history_file)
    logging.info("Loading photo database from flickr...")
    photos = get_photos_from_flickr()
    basepath = image_dir if absolute_path else ''

    flickr_ids = set(photos.keys())
    database_ids = set(get_photo_ids_from_database(history))
    database_paths = set(get_photo_paths_from_database(history))
    filesystem_paths = set(map(lambda x: '/' + os.path.relpath(x, image_dir), images))

    print("%s photos on flickr, %s photos on disk,  %s photos in database" % (len(flickr_ids), len(images), len(database_ids)))

    notinfilesystem = list(database_paths - filesystem_paths)
    notinfilesystem.sort()
    print('####################################################')
    print("%s photos in the database not on the filesystem..." % len(notinfilesystem))
    for path in notinfilesystem:
        print("id=%s: path=%s" % (history[path][0], basepath + path))

    notindatabase = list(filesystem_paths - database_paths)
    notindatabase.sort()
    print('####################################################')
    print("%s photos on the filesystem not in the database..." % len(notindatabase))
    for path in notindatabase:
        print("path=%s" % basepath + path)

    notinflickr = list(database_ids - flickr_ids)
    notinflickr.sort()
    print('####################################################')
    print("%s photos in database not on flickr..." % len(notinflickr))
    for photoid in notinflickr:
        print("id=%s: path=%s" % (photoid, basepath + history[photoid]))

    notindatabase = list(flickr_ids - database_ids)
    notindatabase.sort()
    print('####################################################')
    print("%s photos on flickr not in database..." % len(notindatabase))
    for photoid in notindatabase:
        tags = [tag.raw for tag in photos[photoid].tags]
        hashtags = filter(lambda x: x.startswith('#'), tags)
        if len(hashtags) > 0:
            path = basepath + hashtags[0][1:].replace('#', ' ')
            print('id=%s, path=%s, exists_local=%s' % (photoid, path, os.path.isfile(path)))
        else:
            print('id=%s, tags=%s' % (photoid, ', '. join(tags)))


def reshelf(images, imageDir, historyFile):
    """
    Store image reference in the history file if its not there yet and if we
    actually can find it on Flickr.
    For each file, store the following information:
    - Photo ID from Flickr
    - Modification time
    - Size of file
    """

    logging.debug('flickr2history: Started reshelf')
    try:
        user = flickr.test_login()
        logging.debug(user.id)
    except:
        logging.error(sys.exc_info()[0])
        return None

    for image in images:
        # remove absolute directory
        full_image_path=image
        image = image[len(imageDir):]
        # its better to always reopen this file
        uploaded = shelve.open( historyFile )
        if uploaded.has_key(str(image)):
            if isinstance(uploaded[str(image)], tuple):
                uploaded.close()
                continue
        # each picture should have one id tag in the folder format with spaces
        # replaced by # and starting with #
        flickrtag = '#' + image.replace(' ','#')
        photo = getPhotoIDbyTag(flickrtag, user)
        logging.debug(image)
        logging.debug(photo)
        if not photo:
            uploaded.close()  # flush the DB file
            continue
        logging.debug("flickr2history: Reregistering %s photo "+
                      "in local history file", image)
        stats = os.stat(full_image_path)
        file_mtime=stats.st_mtime
        file_size=stats.st_size
        uploaded[ str(image)] = ( str(photo.id), file_mtime, file_size )
        uploaded[ str(photo.id) ] =str(image)
        uploaded.close()

def delete_photo(filename, image_dir, history_file):
    logging.debug('flickr2history: Started delete_photo')
    try:
        user = flickr.test_login()
        logging.debug(user.id)
    except:
        logging.error(sys.exc_info()[0])
        return None

    path = '/' + os.path.relpath(filename, image_dir) if filename.startswith(image_dir) else filename
    tag = '#' + path.replace(' ', '#')
    
    history = shelve.open(history_file)
    found_in_database = history.has_key(path)
    photos = flickr.photos_search(user_id=user.id, auth=all, tags=tag, tag_mode='any')

    if history.has_key(path) or len(photos) > 0:
        if history.has_key(path):
            photoid, uploaded, filesize = history[path]
            logging.info('Found in database: path=%s, id=%s, uploaded=%s, filesize=%s', path, photoid, uploaded, filesize)
        if len(photos) > 0:
            for photo in photos:
                tags = [tag.raw for tag in photo.tags]
                hashtags = filter(lambda x: x.startswith('#'), tags)
                logging.info('Found photo on flickr: id=%s, tags=%s', photo.id, ', '.join(hashtags if len(hashtags) > 0 else tags))

        delete_confirm = raw_input('Are you sure you want to delete these items (yes/no)? ')
        if delete_confirm.lower() == 'yes' or delete_confirm.lower() == 'y':
            if history.has_key(path):
                photoid, uploaded, filesize = history[path]
                logging.info('Deleting database entry: path=%s, id=%s, uploaded=%s, filesize=%s', path, photoid, uploaded, filesize)
                del history[path]
                if history.has_key(photoid):
                    logging.info('Deleting database entry: id=%s, path=%s', photoid, history[photoid])
                    del history[photoid]
            if len(photos) > 0:
                for photo in photos:
                    logging.info('Deleting photo: %s', photo.id)
                    photo.delete()
        else:
            print('Aborted.')
    else:
        print('Could not find photo in database or on flickr matching: %s' % path)
